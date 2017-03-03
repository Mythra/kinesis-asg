# After this if you don't hate kinesis, I'd be super shocked.
require 'aws-sdk'
require 'slack-notifier'
require 'json'

# Read the configuration file. See kinesis-asg-config.json.
$config = JSON.parse(
  File.read('./kinesis-asg-config.json')
)
if $config['postToSlack']
  $notifier = Slack::Notifier.new $config['slack_webhook_url'],
                                channel: $config['slack_channel'],
                                username: $config['slack_username'],
                                icon_emoji: $config['slack_icon']
end

# The maximum bytes a shard can process per second,
# and the maximum records it can process per second.
$MAX_PER_SHARD_BYTES = 2097152
$MAX_PER_SHARD_RECORDS = 2000

# Determines if an arbitrary item is in an array.
def item_in_array(item, arr)
  !([item] & arr).first.nil?
end

# Determines if a shard has any children.
def child_shard?(shards, shard_id)
  !shards.select { |s| s.parent_shard_id == shard_id }.empty?
end

# Determines which shard has a higher hash range
# for use in determining adjacents.
def determine_high_shard(shard_one, shard_two)
  if shard_one.hash_key_range.starting_hash_key.to_i <
     shard_two.hash_key_range.starting_hash_key.to_i
    [shard_one, shard_two]
  else
    [shard_two, shard_one]
  end
end

def get_open_shards(shards)
  shards.select do |shard|
    shard.sequence_number_range.ending_sequence_number.nil?
  end
end

def check_adjacent_key_range(shards_to_test)
  (shards_to_test[1].hash_key_range.starting_hash_key.to_i -
   shards_to_test[0].hash_key_range.ending_hash_key.to_i) == 1
end

# Get a list of all the adjacent shards.
def get_adjacent_shards(shards)
  final_values = []

  # Loop through the open shards comparing to every other
  # open shard to check for _all_ adjacents.
  shards.each_with_index do |open_shard, index|
    next unless index != (shards.length - 1)
    shards[(index + 1)..shards.length].each do |possibly_adjacent_shard|
      shards_to_test = determine_high_shard(possibly_adjacent_shard, open_shard)
      next unless check_adjacent_key_range(shards_to_test)
      final_values << {
        lower_hash: shards_to_test[0],
        higher_hash: shards_to_test[1]
      }
    end
  end

  final_values
end

def calculate_new_hash_key(shard_to_split)
  # Grab the new starting hash key.
  # This math is straight out of the AWS Docs. Blame them if it breaks.
  starting_hash_key = shard_to_split.hash_key_range.starting_hash_key.to_i
  ending_hash_key = shard_to_split.hash_key_range.ending_hash_key.to_i
  (
    (starting_hash_key + ending_hash_key) / 2
  ).to_s
end

def split_shard(kinesis_client, shard_to_split, stream_name)
  new_starting_hash_key = calculate_new_hash_key(shard_to_split)
  kinesis_client.split_shard(stream_name: stream_name,
                             shard_to_split: shard_to_split.shard_id,
                             new_starting_hash_key: new_starting_hash_key)
  loop do
    described = kinesis_client.describe_stream(stream_name: stream_name)
    break if described.stream_description.stream_status == 'ACTIVE'
    sleep 1
  end
end

def merge_shard(kinesis_client, shard_to_merge, stream_name)
  kinesis_client.merge_shards(
    stream_name: stream_name,
    shard_to_merge: shard_to_merge[:lower_hash].shard_id,
    adjacent_shard_to_merge: shard_to_merge[:higher_hash].shard_id
  )
  loop do
    described = kinesis_client.describe_stream(stream_name: stream_name)
    break if described.stream_description.stream_status == 'ACTIVE'
    sleep 1
  end
end

def perform_shard_scale(stream, use_assume_role, assume_role_creds, scale_get,
  scale_put, scale_puts, metrics_to_grab, metric_results, scaling_up_config,
  scaling_down_config)

  metrics_to_grab += ['IncomingBytes', 'IncomingRecords'] if scale_get
  metrics_to_grab += ['OutgoingBytes', 'OutgoingRecords'] if scale_put || scale_puts
  if metrics_to_grab.empty?
    $notifier.ping "Stream #{stream['name']} has no metrics configured :sadthethings:, Skipping." unless !$notifier
    return
  end

  kinesis_client = nil
  if use_assume_role
    kinesis_client = Aws::Kinesis::Client.new(
      region: stream['region'],
      credentials: assume_role_creds
    )
  else
    kinesis_client = Aws::Kinesis::Client.new region: stream['region']
  end
  begin
    stream_described = kinesis_client.describe_stream(
      stream_name: stream['name']
    )
  rescue Aws::Errors::ServiceError
    return
  end
  if stream_described.stream_description.stream_status != 'ACTIVE'
    $notifier.ping "Stream #{stream['name']} is currently in state: `#{stream_described.stream_description.stream_status}` which is not active, Skipping." unless !$notifier
    return
  end
  stream_shards = get_open_shards(stream_described.stream_description.shards)
  shards_we_can_split = stream_shards.select do |s|
    !child_shard?(stream_shards, s[:shard_id])
  end
  if shards_we_can_split.empty?
    $notifier.ping "Stream #{stream['name']} currently has no open shards to scale up. This should never ever happen. Something is seriously wrong." unless !$notifier
    return
  end

  # Grab Each Metric from cloudwatch
  cloudwatch_client = nil
  if use_assume_role
    cloudwatch_client = Aws::CloudWatch::Client.new(
      region: stream['region'],
      credentials: assume_role_creds
    )
  else
    cloudwatch_client = Aws::CloudWatch::Client.new(
      region: stream['region']
    )
  end
  # Only grab the metrics for the last hour
  current_time = Time.now.to_i
  # Respect Cloudwatch Data Limits
  last_time = current_time - 1440
  stream_shards.each do |shard|
    metrics_to_grab.each do |metric|
      metric_results << {
        shard: shard,
        metric_results: cloudwatch_client.get_metric_statistics(
          namespace: 'AWS/Kinesis',
          metric_name: metric,
          dimensions: [{
            name: 'StreamName',
            value: stream['name']
          }, {
            name: 'ShardID',
            value: shard[:shard_id]
          }],
          start_time: last_time,
          end_time: current_time,
          period: 60,
          statistics: ['Sum']
        )
      }
    end
  end

  per_shard_votes = {}
  metric_results.each do |metric_result|
    metric_shard = metric_result[:shard]
    votes = per_shard_votes[metric_shard[:shard_id]]
    if votes.nil?
      votes = []
    end
    frd_metric_result = metric_result[:metric_results]
    total = 0
    is_bytes = item_in_array(frd_metric_result.label, ['IncomingBytes', 'OutgoingBytes'])
    frd_metric_result.datapoints.each do |datapoint|
      total += datapoint.sum
    end

    avg = total / 1440
    utilization_percentage = 0
    if is_bytes
      utilization_percentage = (avg / $MAX_PER_SHARD_BYTES) * 100
    else
      utilization_percentage = (avg / $MAX_PER_SHARD_RECORDS) * 100
    end

    if utilization_percentage >= scaling_up_config['thresholdPct']
      votes << 'scale_up'
    elsif utilization_percentage <= scaling_down_config['thresholdPct']
      votes << 'scale_down'
    else
      votes << 'scale_same'
    end

    per_shard_votes[metric_shard[:shard_id]] = votes
  end

  per_shard_results = {}
  per_shard_votes.each do |shard_id, votes|
    per_shard_results[shard_id] = votes.group_by { |i| i }
                           .max { |x, y| x[1].length <=> y[1].length }[0]
  end

  non_busy_shards = []
  per_shard_results.each do |shard_id, we_should|
    if we_should == 'scale_up'
      if shards_we_can_split.include?(shard_id)
        if (1 + stream_shards.length) > stream['maxShards']
          $notifier.ping "We want to scale the shard: #{shard_id}, but it's at capcity!" unless !$notifier
          return
        end
        split_shard(kinesis_client, shard_id, stream['name'])
        $notifier.ping "We split the following shard for the Stream: #{stream['name']}:\n `#{shard_id}`" unless !$notifier
      end
      $notifier.ping "We merged #{shards_to_merge.length} shard(s) for the Stream: #{stream['name']}." unless !$notifier
    elsif we_should == 'scale_same'
      $notifier.ping "Shard #{shard_id} is just right. :just_right:" unless !$notifier
    else
      non_busy_shards << shard_id
    end
  end

  if !non_busy_shards.empty?
    $notifier.ping "#{non_busy_shards.length} shards are bored for: #{stream['name']}." unless !$notifier
  end
end

def perform_stream_scale(stream, use_assume_role, assume_role_creds, scale_get,
  scale_put, scale_puts, metrics_to_grab, metric_results, scaling_up_config,
  scaling_down_config)

  metrics_to_grab += ['GetRecords.Bytes', 'GetRecords.Records'] if scale_get
  metrics_to_grab += ['PutRecord.Bytes', 'IncomingRecords'] if scale_put
  metrics_to_grab += ['PutRecords.Bytes', 'PutRecords.Records'] if scale_puts

  if !metrics_to_grab.empty?
    # Only grab the metrics for the last hour
    current_time = Time.now.to_i
    # Respect Cloudwatch Data Limits
    last_time = current_time - 1440

    # Grab Each Metric from cloudwatch
    cloudwatch_client = nil

    if use_assume_role
      cloudwatch_client = Aws::CloudWatch::Client.new(
        region: stream['region'],
        credentials: assume_role_creds
      )
    else
      cloudwatch_client = Aws::CloudWatch::Client.new(
        region: stream['region']
      )
    end

    metrics_to_grab.each do |metric|
      metric_results << cloudwatch_client.get_metric_statistics(
        namespace: 'AWS/Kinesis',
        metric_name: metric,
        dimensions: [{
          name: 'StreamName',
          value: stream['name']
        }],
        start_time: last_time,
        end_time: current_time,
        period: 60,
        statistics: ['Sum']
      )
    end

  else
    $notifier.ping "Stream #{stream['name']} has no metrics configured :sadthethings:, Skipping." unless !$notifier
    return
  end

  kinesis_client = nil
  if use_assume_role
    kinesis_client = Aws::Kinesis::Client.new(
      region: stream['region'],
      credentials: assume_role_creds
    )
  else
    kinesis_client = Aws::Kinesis::Client.new region: stream['region']
  end

  # Describe the Kinesis Stream to get a list of shards, and their info.
  begin
    stream_described = kinesis_client.describe_stream(
      stream_name: stream['name']
    )
  rescue Aws::Errors::ServiceError
    return
  end

  stream_shards = get_open_shards(stream_described.stream_description.shards)
  shards_we_can_split = stream_shards.select do |s|
    !child_shard?(stream_shards, s[:shard_id])
  end

  if stream_described.stream_description.stream_status != 'ACTIVE'
    $notifier.ping "Stream #{stream['name']} is currently in state: `#{stream_described.stream_description.stream_status}` which is not active, Skipping." unless !$notifier
    return
  end

  if shards_we_can_split.empty?
    # This can only happen if someone manually splits a stream,
    # and then messes with the parent ids.
    $notifier.ping "Stream #{stream['name']} currently has no open shards to scale up. This should never ever happen. Something is seriously wrong." unless !$notifier
    return
  end

  scale_votes = []
  # Grab the maximum bytes/records our stream can handle.
  max_bytes = $MAX_PER_SHARD_BYTES * stream_shards.length
  max_records = $MAX_PER_SHARD_RECORDS * stream_shards.length

  # The way this works is each metric gets a vote of whether it
  # needs more resources, less resources, or is okay where it's at.
  # Whichever gets the most votes wins.
  metric_results.each do |result|
    total = 0
    is_bytes = item_in_array(result.label,
      ['GetRecords.Bytes', 'PutRecord.Bytes', 'PutRecords.Bytes'])

    result.datapoints.each do |datapoint|
      total += datapoint.sum
    end

    avg = total / 1440
    utilization_percentage = 0

    # Utilization Percentage is exactly what it sounds like.
    # How much of this metric are we using.
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
  end

  if scale_votes == []
    $notifier.ping "That's weird. Stream: #{stream['name']} had no scaling votes, Skipping." unless !$notifier
    return
  end

  we_should = scale_votes.group_by { |i| i }
                         .max { |x, y| x[1].length <=> y[1].length }[0]

  if we_should == 'scale_up'
    if (scaling_up_config['rate'] + stream_shards.length) > stream['maxShards']
      $notifier.ping "We want to scale: #{stream['name']}, but it's at capcity!" unless !$notifier
      return
    end
    shards_to_split = shards_we_can_split.sample(scaling_up_config['rate'])
    begin
      shards_to_split.each do |shard_to_split|
        split_shard(kinesis_client, shard_to_split, stream['name'])
      end
    rescue Aws::Errors::ServiceError
      return
    end
    $notifier.ping "We split the following shards for the Stream: #{stream['name']}:\n ```#{shards_to_split.join(', ')}```" unless !$notifier
  elsif we_should == 'scale_down'
    return if stream_shards.length == 1
    if (stream_shards.length - scaling_down_config['rate']) < stream['minShards']
      $notifier.ping "We want to turndown: #{stream['name']}, but your config says not to." unless !$notifier
      return
    end
    adjacent_shards = get_adjacent_shards(stream_shards)
    # If your adjacent shards aren't yet open, or other stuff is going on it's possible that you
    # can't merge any shards. This sucks, but from reading it's a decently common occurence.
    if adjacent_shards.empty?
      $notifier.ping "We want to merge shards for the Stream: #{stream['name']}, but it has no adjacent shards. :tear:." unless !$notifier unless !$notifier
      return
    end

    shards_to_merge = adjacent_shards.sample(scaling_down_config['rate'])
    begin
      shards_to_merge.each do |to_merge|
        merge_shard(kinesis_client, to_merge, stream['name'])
      end
    rescue Aws::Errors::ServiceError
      return
    end
    $notifier.ping "We merged #{shards_to_merge.length} shard(s) for the Stream: #{stream['name']}." unless !$notifier
  elsif we_should == 'scale_same'
    $notifier.ping "Stream #{stream['name']} is just right :just_right:." unless !$notifier
  else
    $notifier.ping "Stream #{stream['name']} doesn't know how to scale: `#{we_should}`. :sadthethings:." unless !$notifier
  end
end

loop do
  accounts = $config['accounts']

  $config['streams'].each { |stream|
    scaling_config = stream['scalingConfig']
    scaling_up_config = scaling_config['up']
    scaling_down_config = scaling_config['down']
    scale_get = item_in_array(stream['scaleOn'], ['GET', 'ALL'])
    scale_put = item_in_array(stream['scaleOn'], ['PUT', 'BOTH_PUTS', 'ALL'])
    scale_puts = item_in_array(stream['scaleOn'], ['PUTS', 'BOTH_PUTS', 'ALL'])

    account_to_use = stream['account']
    account_config = accounts[account_to_use]
    use_assume_role = false
    assume_role_creds = nil

    if account_config['assume_role']
      use_assume_role = true
      assume_role_creds = Aws::AssumeRoleCredentials.new(
        client: Aws::STS::Client.new,
        role_arn: account_config['role_arn'],
        role_session_name: account_config['role_session_name']
      )
    end

    if stream['shardLevelScale']
      perform_shard_scale(stream, use_assume_role, assume_role_creds,
        scale_get, scale_put, scale_puts, [], [], scaling_up_config,
        scaling_down_config)
    else
      perform_stream_scale(stream, use_assume_role, assume_role_creds,
        scale_get, scale_put, scale_puts, [], [], scaling_up_config,
        scaling_down_config)
    end
  }
  sleep $config['cloudwatchRefreshTime']
end