# To compile
mvn compile

# To Run
mvn exec:java -Dexec.mainClass="com.wisr.mlsched.Simulation" -Dexec.args="<Number of racks> <Number of machines in a rack> <Scheduler (Dally/Tiresias/Gandiva)> <cluster configuration file> <Job trace file> <Astra-sim network file> <Astra-sim system file> <Astra-sim workload file> <job label (name of result directory)> <network delay time (-1 for auto tune)> <rack delay time (-1 for auto tune)> <Initial history size (hours)>


