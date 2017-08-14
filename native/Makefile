

JAVA = /usr/lib/jvm/java-8-openjdk/include

build: 
	gcc -I'${JAVA}' \
	    -I'${JAVA}/linux' \
	    -Wall -Werror \
	    -fPIC \
	    -c BucketBufferArray.h BucketBufferArray.c



shared: build
	gcc -shared -o libnativeMap.so BucketBufferArray.o

copy: shared
	cp libnativeMap.so ../jnative/src/main/resources/lib

all: build shared copy
