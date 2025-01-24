## How to run?
Install JDK using sdk man:
```
sdk install java 21.0.5-graal
```
Install hyperfine:
```
brew install hyperfine
```
Copy `pool.csv` file into the project's root directory.

To run different solutions use classname as parameter in `evaluate.sh <classname>`:
```
./evaluate.sh Main
```
## Solutions

### Using Java Memory Api (preview in Java 21)
Run `evaluate.sh Main`. This solution doesn't work in native mode since Graalvm doesn't support Java Memory Api for ARM platforms yet. It should work in AMD x64 platforms, but I haven't tried it.

### Using UNSAFE
Run `evaluate.sh MainUnsafe`. This solution is just to push the limits for fun. It is also fully compatible and works in Graalvm Native.
For native run `evaluate.sh MainUnsafe --native`

