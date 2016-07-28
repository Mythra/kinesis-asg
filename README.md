# Kinesis-ASG #

Kinesis-ASG is an "auto-scaling group" for Kinesis. Essentially it monitors cloudwatch metrics for
any number of streams, and determines their "Usage". If it notices their usage is "low" (as set up
in your config), it will try to scale them down. If it notices their usage is "high" (as also set
up in your config), it will scale them up. You can also set a minimum/maximum limit for each stream.

## Usage ##

In order to use kinesis-asg you'll need to do a couple things:
1. Fill out the config. Example: `cp kinesis-asg-config.json.example kinesis-asg-config.json`, and fill it out.
2. Set an upstart config to run the ruby script `kinesis-asg-runner` on start.
3. Start the runner for the first time manually. I.E. `bundle install && bundle exec ruby kinesis-asg-runner.rb start`

## Slack Usage ##

Kinesis-ASG can post when it scales/can't scale to slack. You can set this up through the configuration.
All you need to do this is a webhook url for slack. You can find info about it [here][slack_link].

## Note About Shards ##

Sometimes when you're auto-scaling your streams you'll start noticing read/write errors,
_even though_ your stream seems to be over-provisioned. This is because the stream can write to a
specific shard(s) very heavily. If we scale those down, it could cause errors to occur. In this case,
we recommend setting a higher minimum amount of shards.

If you're still running into problems with shards we recommend setting up an "ignored shards list",
where you can create an array of shard ids to ignore when scaling down.

[slack_link]: https://api.slack.com/incoming-webhooks
