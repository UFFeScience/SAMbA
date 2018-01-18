echo "NAME;FASTA_FILE" > inputList.txt
echo "ORTHOMCL1;$WORKSPACE/inputs/ORTHOMCL1" >> inputList.txt

$SPARK_HOME/bin/spark-submit \
  --class Main \
  --master local[4] \
  SciPhySpark.jar \
  inputList.txt
