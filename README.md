# Python (Optimized) vs Go using [mysqldump2csv](https://github.com/bramp/mysqldump2csv) 
**One process and probably one thread. Produce 102 GB (Unfiltered data). 12 Hours of processing in Go** \
![12 hrs processing](./assets/12hrs-processing.png)

**Multiprocessing in Python** \
![a](./assets/multiprocessing-python.jpg)

**10 min only! produce 18 GB (Filtered data) in Python** \
![aa](./assets/processing-time.jpg)

What I learn?
- Maximize hardware capabilities. Use Multiprocess, Thread, Async, etc. (Don't process in series)
- Split the data (Chunking) and let each process in multiprocess handle it.
- I/O operation is exspensive. Buffer...
- Don't waste your time to use "Fast" programming language. Probably better if u can....