

JAVA = /usr/lib/jvm/java-8-openjdk/include

build: 
	gcc -I'${JAVA}' \
	    -I'${JAVA}/linux' \
	    -Wall -Werror \
	    -fPIC \
	    -c *.h *.c



shared: build
	gcc -shared -o libnativeMap.so *.o

copy: shared
	cp libnativeMap.so ../jnative/src/main/resources/lib

all: build shared copy
