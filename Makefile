OUT_ROOT=out
CLASSES_PATH=${OUT_ROOT}/classes
ARTIFACTS_PATH=${OUT_ROOT}/artifacts

JAR_NAME=${ARTIFACTS_PATH}/build-plugin.jar
RESOURCES_PATH=src/main/resources
LIB_PATH=lib

TEST_FILE=src/test/scala/org/so/plugin/BroadcastJoinSuite.scala

INPUT_PATH=input/big/
OUTPUT_PATH=output/big/
STATS_PATH=output/stats/

all: build run

build: setup
	mkdir -p "out/classes/main/resources/"
	mkdir -p ${STATS_PATH}
	${SCALA_BIN_PATH}scalac -cp "./${LIB_PATH}/*" \
		-d out/classes \
		src/main/scala/org/neu/so/bj/*.scala \
		src/test/scala/org/neu/so/bj/util/*.scala \
		src/test/scala/org/neu/so/bj/*.scala
	jar cvf ${JAR_NAME} \
		-C out/classes/ .

build-jar: setup
	mkdir -p "out/classes/main/resources/"
	${SCALA_BIN_PATH}scalac -cp "./${LIB_PATH}/*" \
    		-d out/classes \
    		src/main/scala/org/neu/so/bj/*.scala
	jar cvf ${JAR_NAME} \
		-C out/classes/ .

run:
	${SPARK_PATH}spark-submit \
		--master local --driver-memory 6g \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
		--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
		--class org.neu.so.bj.BroadcastJoinSuite ${JAR_NAME} ${INPUT_PATH} ${OUTPUT_PATH}

debug: build
	scalac -Xplugin:${JAR_NAME} -Xprint:all ${TEST_FILE}

debug-browse: build
	scalac -Xplugin:${JAR_NAME} -Ybrowse:all ${TEST_FILE}

setup: clean
	mkdir -p ${CLASSES_PATH}
	mkdir -p ${ARTIFACTS_PATH}

clean:
	rm -rf ${OUT_ROOT}
	rm -rf ${OUTPUT_PATH}
	rm -rf ${STATS_PATH}
