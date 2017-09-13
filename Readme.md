## Requirements
* JRE 8
* maven 3
* Groovy >= 2.4.7

## Build
1. recoursively clone the repository  (you also need to fetch the submodule)
2. build the `hierarchical-clustering-java` module 
```
cd hierarchical-clustering-java
mvn clean install
```
3. build the main module
```
cd ..
./gradlew clean install
```
4. now everything is in maven cache and we can simply run the groovy script
```
cd build/scripts
groovy HierarchicalRiskPortfolio.groovy
```
