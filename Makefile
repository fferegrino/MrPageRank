.ONESHELL:
run: clean package
package:
	mvn -f wiki/pom.xml package
clean:
	mvn -f wiki/pom.xml clean