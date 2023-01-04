#!/bin/bash

spark_submit=/spark/bin/spark-submit
packages=org.postgresql:postgresql:42.2.5
jars=/home/scala/target/scala-2.12/jdp.jar
email_jars=/home/scala/target/scala-2.12/jdp.jar,/home/scala/target/scala-2.12/Scala_Spark_Mail.jar
class_products1="com.example.ProductsFromLocalToHDFS"
class_products2="com.example.ProductsFromHDFSToHive"
class_countries1="com.example.CountriesFromInvoicesCsvToHDFS"
class_countries2="com.example.CountriesFromHDFSToHive"
class_invoices1="com.example.InvoicesFromLocalToHDFS"
class_invoices2="com.example.InvoicesFromHDFSToHive"
class_join="com.example.Join"
master=local[2]
end=/home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

echo "50 8 * * * $spark_submit --jars $jars --class $class_products1 --master $master $end" > /var/spool/cron/crontabs/root 
echo "51 8 * * * $spark_submit --jars $jars --class $class_products2 --master $master $end" >> /var/spool/cron/crontabs/root
echo "52 8 * * * $spark_submit --jars $jars --class $class_countries1 --master $master $end" >> /var/spool/cron/crontabs/root
echo "53 8 * * * $spark_submit --jars $jars --class $class_countries2 --master $master $end" >> /var/spool/cron/crontabs/root
echo "55 * * * * $spark_submit --jars $jars --class $class_invoices1 --master $master $end" >> /var/spool/cron/crontabs/root
echo "57 */4 * * * $spark_submit --jars $jars --class $class_invoices2 --master $master $end" >> /var/spool/cron/crontabs/root 
echo "59 */4 * * * $spark_submit --packages $packages --jars $email_jars --class $class_join --master $master $end" >> /var/spool/cron/crontabs/root
crond start
