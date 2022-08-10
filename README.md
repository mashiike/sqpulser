# sqpulser
sqpulser is a tool for compiling SQS messages and emitting them in a pulsatile cycle

## Usage

```
sqpulser -in sqpulser-in -out sqpulser-out -emit-interval 1h -offset 15m
2022/08/10 12:14:30 [info] try get incoming queue url: queue name `sqpulser-in`
2022/08/10 12:14:31 [info] try get outgoing queue url: queue name `sqpulser-out`
2022/08/10 12:14:31 [info] start polling: https://sqs.ap-northeast-1.amazonaws.com/012345678900/sqpulser-in
```

If SendMessage is sent at 12:40, 12:41, or 12:43, ReciveMessage can be sent at 13:15 from the outgoing queue.
This is an application that chunks messages sent to a specified incoming queue at a certain time, and summarizes them so that they will be recived at a specific time.

## LICENSE

MIT
