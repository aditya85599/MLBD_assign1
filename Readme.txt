#Installing Hadoop on macOS

This guide explains how to install Hadoop 3.3.6 on macOS and configure Java properly.

    1. Prerequisites: Check Java Installation
        Before installing Hadoop, verify whether Java is installed:
        java -version
        If Java is already installed, proceed to the next step.
    If not installed, install OpenJDK using Homebrew:
    brew install openjdk
        After installation, add OpenJDK to your PATH:
            echo 'export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"' >> ~/.zshrc
            source ~/.zshrc

    2. Download Hadoop
        Download Hadoop 3.3.6:
        curl -O https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

3. Extract Hadoop
tar -xzf hadoop-3.3.6.tar.gz

4. Move Hadoop to Local Directory
Move Hadoop to /usr/local:
sudo mv hadoop-3.3.6 /usr/local/hadoop
Enter your system password if prompted.

5. Verify Hadoop Installation
Check the Hadoop version:
hadoop version
If installed correctly, the version information will be displayed.

6. Start Hadoop Services
Start HDFS:
start-dfs.sh
Start YARN:
start-yarn.sh


##Important Note on Java Versions
Hadoop 3.3.6 is compatible with Java 11, while Apache Spark typically requires Java 17.
To avoid runtime issues:
Install both Java 11 and Java 17.
Configure environment variables appropriately.
Ensure the correct Java version is active when running Hadoop or Spark.
You can switch Java versions by updating the JAVA_HOME environment variable before starting each service.
Example:
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
For Spark:
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
