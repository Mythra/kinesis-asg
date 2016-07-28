# After this if you don't hate kinesis, I'd be super shocked.
require 'aws-sdk'
require 'slack-notifier'
require 'json'

#Read the configuration file. See hot-tub-kinesis-config.json.
$config = JSON.parse(File.read('./kinesis-asg-config.json'))

if $config['postToSlack']
  $notifier = Slack::Notifier.new $config['slack_webhook_url'],
                                channel: $config['slack_channel'],
                                username: $config['slack_username'],
                                icon_emoji: $config['slack_icon']
end

$MAX_PER_SHARD_BYTES = 2097152
$MAX_PER_SHARD_RECORDS = 2000

def item_in_array(item, arr)
  !!([item] & arr).first
end

def has_child_shard(shards,shard_id)
  shards.select{ |s| s.parent_shard_id == shard_id}.length > 0
end

def determine_high_shard(shard_one, shard_two)
  if shard_one.hash_key_range.starting_hash_key.to_i < shard_two.hash_key_range.starting_hash_key.to_i
    return [shard_one, shard_two]
  else
    return [shard_two, shard_one]
  end
end

def get_open_shards(shards)
  return shards.select { |shard|
    shard.sequence_number_range.ending_sequence_number == nil
  }
end

def get_adjacent_shards(shards)
  final_values = []

  shards.each_with_index { |open_shard, index|
    if index != shards.length - 1
      shards[(index+1)..shards.length].each { |possibly_adjacent_shard|
        shards_to_test = determine_high_shard(possibly_adjacent_shard, open_shard)
        if (shards_to_test[1].hash_key_range.starting_hash_key.to_i -
          shards_to_test[0].hash_key_range.ending_hash_key.to_i) == 1
          final_values << {
            :lower_hash => shards_to_test[0],
            :higher_hash => shards_to_test[1]
          }
        end
      }
    end
  }

  final_values
end

loop do
  $config['streams'].each { |stream|
    scaling_config = stream['scalingConfig']
    scaling_up_config = scaling_config['up']
    scaling_down_config = scaling_config['down']
    scale_get = item_in_array(stream['scaleOn'], ['GET', 'ALL'])
    scale_put = item_in_array(stream['scaleOn'], ['PUT', 'BOTH_PUTS', 'ALL'])
    scale_puts = item_in_array(stream['scaleOn'], ['PUTS', 'BOTH_PUTS', 'ALL'])
    metrics_to_grab = []
    metric_results = []

    if scale_get
      metrics_to_grab += ['GetRecords.Bytes', 'GetRecords.Success']
    end
    if scale_put
      metrics_to_grab += ['PutRecord.Bytes', 'PutRecord.Success']
    end
    if scale_puts
      metrics_to_grab += ['PutRecords.Bytes', 'PutRecords.Records']
    end

    if metrics_to_grab.length > 0
      current_time = Time.now.to_i
      last_time = current_time - 1440

      metrics_to_grab.each { |metric|
        metric_results << Aws::CloudWatch::Client.new(region: stream['region']).get_metric_statistics(
          namespace: 'AWS/Kinesis',
          metric_name: metric,
          dimensions: [{
            name: "StreamName",
            value: stream['name']
          }],
          start_time: last_time,
          end_time: current_time,
          period: 60,
          statistics: ['Sum']
        )
      }

    else
      $notifier.ping "Stream #{stream['name']} has no metrics configured :sadthethings:, Skipping." unless !$notifier
      next
    end

    kinesis_client = Aws::Kinesis::Client.new region: stream['region']

    begin
      stream_described = kinesis_client.describe_stream({
        stream_name: stream['name']
      })
    rescue Aws::Errors::ServiceError
      next
    end

    stream_shards = get_open_shards(stream_described.stream_description.shards)
    shards_we_can_split = stream_shards.select{ |s| !has_child_shard(stream_shards, s[:shard_id]) }

    if stream_described.stream_description.stream_status != 'ACTIVE'
      $notifier.ping "Stream #{stream['name']} is currently in state: `#{stream_described.stream_description.stream_status}` which is not active, Skipping." unless !$notifier
      next
    end

    if shards_we_can_split.length == 0
      $notifier.ping "Stream #{stream['name']} currently has no open shards to scale up. This should never ever happen. Something is seriously wrong." unless !$notifier
      next
    end

    scale_votes = []
    max_bytes = $MAX_PER_SHARD_BYTES * stream_shards.length
    max_records = $MAX_PER_SHARD_RECORDS * stream_shards.length

    metric_results.each { |result|
      total = 0
      is_bytes = item_in_array(result.label, ['GetRecords.Bytes', 'PutRecord.Bytes', 'PutRecords.Bytes'])

      result.datapoints.each { |datapoint|
        total += datapoint.sum
      }

      avg = total / 1440
      utilization_percentage = 0

      if is_bytes
        utilization_percentage = (avg / max_bytes) * 100
      else
        utilization_percentage = (avg / max_records) * 100
      end

      if utilization_percentage >= scaling_up_config['thresholdPct']
        scale_votes << 'scale_up'
      elsif utilization_percentage <= scaling_down_config['thresholdPct']
        scale_votes << 'scale_down'
      else
        scale_votes << 'scale_same'
      end
    }

    if scale_votes == []
      $notifier.ping "That's weird. Stream: #{stream['name']} had no scaling votes, Skipping." unless !$notifier
      next
    end

    we_should = scale_votes.group_by{|i| i}.max{|x,y| x[1].length <=> y[1].length}[0]

    if we_should == 'scale_up'
      if (stream_shards.length + 1) > stream['maxShards']
        $notifier.ping "We want to scale: #{stream['name']}, but it's at capcity!" unless !$notifier
        next
      end
      shard_to_split = shards_we_can_split.sample(1)
      begin
        shard_to_split.each { |shard_to_split|
          # Grab the new starting hash key. This math is straight out of the AWS Docs. Blame them if it breaks.
          new_starting_hash_key = ((shard_to_split.hash_key_range.starting_hash_key.to_i + shard_to_split.hash_key_range.ending_hash_key.to_i) / 2).to_s
          kinesis_client.split_shard(:stream_name => stream['name'],
                                     :shard_to_split => shard_to_split.shard_id,
                                     :new_starting_hash_key => new_starting_hash_key)
        }
      rescue Aws::Errors::ServiceError
        next
      end
      $notifier.ping "We split the following shards for the Stream: #{stream['name']}:\n ```#{shards_to_split.join(', ')}```" unless !$notifier
    elsif we_should == 'scale_down'
      if stream_shards.length == 1
        $notifier.ping "We want to turndown: #{stream['name']}, but it only has 1 shard." unless !$notifier
        next
      end
      if (stream_shards.length - 1) < stream['minShards']
        $notifier.ping "We want to turndown: #{stream['name']}, but your config says not to." unless !$notifier
        next
      end
      adjacent_shards = get_adjacent_shards(stream_shards)
      if adjacent_shards.length == 0
        $notifier.ping "We want to merge shards for the Stream: #{stream['name']}, but it has no adjacent shards. :tear:." unless !$notifier
        next
      end

      shard_to_merge = adjacent_shards.sample(1)
      if scalingConfig['ignoreShardsForScaleDown']
        if item_in_array(scalingConfig['ignoreShardsForScaleDown'], shard_to_merge[0])
          10.times do
            shard_to_merge = adjacent_shards.sample(1)
            if !item_in_array(scalingConfig['ignoreShardsForScaleDown'], shard_to_merge[0])
              break
            end
          end
          if item_in_array(scalingConfig['ignoreShardsForScaleDown'], shard_to_merge[0])
            $notifier.ping "We've grabbed 11 shards, and all of them are ignored. Skipping for now."
            next
          end
        end
      end
      begin
        shard_to_merge.each { |to_merge|
          kinesis_client.merge_shards({
            stream_name: stream['name'],
            shard_to_merge: to_merge[:lower_hash].shard_id,
            adjacent_shard_to_merge: to_merge[:higher_hash].shard_id
          })
        }
      rescue Aws::Errors::ServiceError
        next
      end
      $notifier.ping "We merged #{shards_to_merge.length} shard(s) for the Stream: #{stream['name']}." unless !$notifier
    elsif we_should == 'scale_same'
      $notifier.ping "Stream #{stream['name']} is just right :just_right:." unless !$notifier
    else
      $notifier.ping "Stream #{stream['name']} doesn't know how to scale: `#{we_should}`. :sadthethings:." unless !$notifier
    end
  }
  sleep $config['cloudwatchRefreshTime']
end
