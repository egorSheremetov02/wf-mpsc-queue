

I decided to implement MPSC queue inspired by this academic paper: https://arxiv.org/pdf/2010.14189

It has wait-free guarantee and is more cache efficient compared to some analogues like Michael-Scott queue.

It is also more memory efficient compared to its MPMC counterparts (such as FAA-queue for instance which is also the
closest to this one in terms of the design)

When testing the queue I only was able to get seemingly incorrect tests generated my Lincheck.

For example

```text
= Invalid execution results =
| ------------------------------------ |
|     Thread 1     |     Thread 2      |
| ------------------------------------ |
| enqueue(1): void |                   |
| dequeue(): 1     |                   |
| ------------------------------------ |
| dequeue(): null  | enqueue(-1): void |
| ------------------------------------ |
```

To me this seems like perfectly correct execution. Lincheck, however, considers it to be erroneous for some reason.



