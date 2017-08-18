java -Djava.library.path=src/main/resources/lib/ \
  -agentpath:/home/zell/profiler/liblagent.so=interval=7,logPath=/tmp/perf-log.hpl -Dfile.encoding=UTF-8 \
  -cp target/jnative-1.0-SNAPSHOT-jar-with-dependencies.jar \
  de.zell.jnative.ZbMapMain
