#!/bin/bash

JAVA_VERSION="21.0.6-graal"
INPUT_ARGS="9 13 40 34 17 7" # 9 13 40 34 17 7

# Handle positional arguments
shift $((OPTIND - 1))
param1=$1
param2=$2

if [ "$param1" = "" ]; then
  echo "Usage: $0 <classname>"
  exit 1
fi

echo "using $JAVA_VERSION"

# set java version
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java $JAVA_VERSION

# java compile
"$HOME"/.sdkman/candidates/java/$JAVA_VERSION/bin/javac --release "$(echo $JAVA_VERSION | cut -d. -f1)" --enable-preview -d ./bin ./src/"$param1".java

if [ "$param2" == "--native" ]; then
    NATIVE_IMAGE_OPTS="--initialize-at-build-time=$param1 -O3 -march=native -R:MaxHeapSize=128m -H:-GenLoopSafepoints --enable-preview" # --gc=epsilon
    native-image $NATIVE_IMAGE_OPTS -cp ./bin "$param1"
fi

TIMEOUT="gtimeout -v 30" # in seconds, from `brew install coreutils`
HYPERFINE_OPTS="--warmup 5 --runs 5 --export-json timing.json --output ./$param1.out" # --show-output

if [ "$param2" == "--native" ]; then
    imageName=$(echo "$param1" | tr '[:upper:]' '[:lower:]')
    echo "Picking up native image './$imageName'" 1>&2
    hyperfine $HYPERFINE_OPTS "$TIMEOUT ./$imageName $INPUT_ARGS"
else
    JAVA_OPTS="-Xmx64m --enable-preview"
    echo "Choosing to run the app in JVM mode" 1>&2
    hyperfine $HYPERFINE_OPTS "$TIMEOUT sh -c '$HOME/.sdkman/candidates/java/$JAVA_VERSION/bin/java $JAVA_OPTS -classpath ./bin $param1 $INPUT_ARGS'"
fi
