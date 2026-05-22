all: compile-client

../Server/RMIInterface.jar: ../Server/Server/Interface/IResourceManager.java
	make -C ../Server/ RMIInterface.jar

# For compile-client: javac -cp ../Server/RMIInterface.jar:../Server Client/*.java
compile-client: ../Server/RMIInterface.jar
	javac -cp ../Server/RMIInterface.jar:../Server Client/*.java

clean:
	rm -f Client/*.class
	rm -f ../Server/RMIInterface.jar
