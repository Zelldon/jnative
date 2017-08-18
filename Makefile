
JAVA = /usr/lib/jvm/java-8-openjdk/include

.PHONY: clean all

clean:
	rm -f ./*.o

javah: native/src/de_zell_jnative_BucketBufferArray.h
	javah -classpath jnative/src/main/java -d native/src de.zell.jnative.BucketBufferArray

ccompile: javah
	gcc -I'${JAVA}' \
	    -I'${JAVA}/linux' \
	    -Wall -Werror \
	    -fPIC \
	    -Ofast \
	    -c native/src/*.h native/src/*.c

shared-lib: ccompile
	gcc -shared -o libnativeMap.so *.o

mv-lib: shared-lib
	mv libnativeMap.so jnative/src/main/resources/lib

jcompile: mv-lib
	cd jnative && mvn clean install

all: clean javah ccompile shared-lib mv-lib jcompile clean

build: all
