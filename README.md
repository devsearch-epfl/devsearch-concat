ParallelConcat
==============

Concatenate source files in parallel from the DevMine source repositories

Build & Run
-----------

```
> sbt assembly
> java -jar target/scala-2.11/ParallelConcat-assembly-1.0.jar [-j=<numJobs>] <REPO_ROOT> <OUTPUT_FOLDER> 
```
