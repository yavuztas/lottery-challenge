#!/bin/bash

JAVA_VERSION="21.0.5-graal"
INPUT_ARGS="9 13 40 34 17 7"

# java compile
"$HOME"/.sdkman/candidates/java/$JAVA_VERSION/bin/javac --release 21 --enable-preview -d ./bin ./src/Main.java

# Handle positional arguments
shift $((OPTIND - 1))
param1=$1

if [ "$param1" == "--native" ]; then
#    NATIVE_IMAGE_OPTS="--initialize-at-build-time=MainExperimental --gc=epsilon -O3 -march=native -R:MaxHeapSize=128m -H:-GenLoopSafepoints --enable-preview"
    NATIVE_IMAGE_OPTS="--enable-preview"
    native-image $NATIVE_IMAGE_OPTS -cp ./bin Main
fi

TIMEOUT="gtimeout -v 30" # in seconds, from `brew install coreutils`
HYPERFINE_OPTS="--warmup 5 --runs 5 --export-json timing.json --output ./main.out"

if [ "$param1" == "--native" ]; then
    echo "Picking up existing native image './main', delete the file to select JVM mode." 1>&2
    hyperfine $HYPERFINE_OPTS "$TIMEOUT ./main $INPUT_ARGS"
else
    JAVA_OPTS="-Xmx64m -XX:MaxGCPauseMillis=1 -XX:-AlwaysPreTouch -XX:+UseSerialGC -XX:+TieredCompilation --enable-preview"
    echo "Choosing to run the app in JVM mode as no native image was found" 1>&2
    hyperfine $HYPERFINE_OPTS "$TIMEOUT sh -c '$HOME/.sdkman/candidates/java/$JAVA_VERSION/bin/java $JAVA_OPTS -classpath ./bin Main $INPUT_ARGS'"
fi




