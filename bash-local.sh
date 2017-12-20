EC2=$1
echo "you entered $EC2"
if [ $EC2 != "" ]
then
scp -i ~/.ssh/betsol-hadoop.pem customers-with-header-500.csv  hadoop@$EC2:
scp -i ~/.ssh/betsol-hadoop.pem customers-with-out-header-500.csv  hadoop@$EC2:
scp -i ~/.ssh/betsol-hadoop.pem aws-emr-hbase-test.py  hadoop@$EC2:
scp -i ~/.ssh/betsol-hadoop.pem bash-aws.sh  hadoop@$EC2:
fi