# 项目背景
当前许多大数据平台上的数据都是通过周期性跑批的方式将mysql中的数据
抽取到Hive平台中,这种方式时效性比较低,因此需要用一种近实时的方式将
mysql中的变更数据应用到大数据平台中(hive及hbase)。
# 解决方案
要达到将mysql中的变更数据实时应用到大数据平台的目标,首先需要监控到mysql
的数据变更,目前成熟的解决方案有阿里的canal项目,因此我们就使用了canal,
并且我们实现了一个MR程序将canal里的mysql变更数据写到了kafka中。另外,
我们还需要将变更数据同步到hive或是hbase,这里我们采用的是spark streaming方案,
通过spark streaming将kafka中的mysql变更数据同步到hive和hbase。在实际开发
的时候,我们发现HDP平台自身的hive服务和spark服务的jar包不兼容,因此我们改为了实现一个
MR程序来将kafka里的数据同步到hive。这里我们使用了Hive HCatalog Streaming API,但是该api
只能处理新增数据,因此对于update和delete的情况我们采用了通过JDBC连接执行update、delete语句
来处理(hive 2.0以上可以采用Streaming Mutation API处理)。此外,由于hive自身能力所限,
该方案只能同步数据到开了事务特性的orc表中,hive事务相关内容参看
https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions, Hive HCatalog Streaming API
相关内容参看https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest

# 项目地址
https://git.zhubajie.la/zhangxiaofeng/pipeline

# 代码结构说明
根据上文所诉,本项目包含3个独立的程序:同步canal数据到kafka的MR程序、同步kafka数据到hbase
的spark streaming程序以及同步kafka数据到hive的MR程序,下面对这3个程序做详细说明。

### 同步canal数据到kafka的MR程序
该程序的入口为Canal2Kafka.java类,该程序比较简单,就是运行一个mapper监听canal里的数据变化,
然后调用kafka的生产者api将这些数据发到kafka中。发到kafka中的数据的key的格式为:schemaName|tableName|rowkeys,
之所以这样设计key,主要是为了保证对于同一条数据的更新操作会存到相同的partition,对于同一个partition里
的数据,kafka是保证被客户端顺序消费的,这样的话,我们就可以保证对于同一条数据的更新操作是按顺序重放的。发到kafka中
的数据的value的格式为:database={schemaName},table={tableName},eventType={变更类型}  id={变更后的值},type={数据类型},update={true/false},isKey={true/false},
一个真实的示例形如:
database=ottertest,table=test,eventType=DELETE	id=20150,type=int(11),update=false,isKey=true	name=snow,type=varchar(255),update=false,isKey=false	password=BIGdata_2013,type=varchar(255),update=false,isKey=false	email=abc@zbj.com,type=varchar(255),update=false,isKey=false	phone=15618378988,type=varchar(255),update=false,isKey=false	time=2017-12-28 10:07:49,type=datetime,update=false,isKey=false

### 同步kafka数据到hbase的spark streaming程序
该程序的入口为StreamingJob.java,入口类的主要功能是拿到kafka中的数据,将这些数据交给对应
的HBaseWriter处理。为了保证对于同一条数据的顺序处理,相同的key的数据只会由同一个HBaseWriter处理。

### 同步kafka数据到hive的MR程序
该程序的入口为Kafka2Hive.java。该程序可以启动多个mapper同时处理,每个mapper内部也可以启动
多个HiveWriter,大大的提高了并发度。和写hbase的spark streaming类似,对于同一条数据的变更操作也只会由
同一个HiveWriter处理,保证变更操作按顺序回放。

# 配置文件说明
本项目只有一个pipeline.properties配置文件,包含了上面所提到的3个程序的配置。


参数名|说明
----------|------
canal.host|canal服务地址
canal.port|canal服务端口
canal.destination|监听的destination,也就是一个mysql数据库
canal.zookeeper.urls|canal服务所使用的zookeeper地址,采用集群模式的canal时会用到
kafka.bootstrap.servers|kafka broker地址
kafka.zookeeper.connect|kafka所使用的zookeeper地址
kafka.topic| topic名
kafka.hbase.group.id|同步kafka数据到hbase的spark streaming程序所使用的group名
kafka.hive.group.id|同步kafka数据到hive的MR程序所使用的group名
kafka.record.database|我们所需要的的变更数据所在的mysql数据库名
kafka.record.tablename|需要监控变更的mysql表名
streaming.receiver.num|流处理启动的receiver数
streaming.batchInterval|流处理程序的batch interval,单位s
isDirect=true|是否使用direct api
hbase.tablename|同步变更到hbase中额哪张表中
hbase.writer.number|启动多少个HBaseWriter
hive.metastore.url|hive metastore地址
hive.database.name|同步变更数据到hive的哪个数据库中
hive.tablename|同步变更数据到hive的哪张表中
hive.table.columnNames|hive中表的列明,逗号分隔
hive.table.primaryKeys|hive中表的主键列名,delete和update时需要用该列做过滤
hive.server.url|hive jdbc url
hive.username|登录hive的用户名
hive.password|登录hive的密码
hive.writer.number|启动多少个HiveWriter
# 安装部署说明
假设安装目录为/home/spark/zhangxiaofeng/pipeline,在该目录下新建bin、conf、lib3个目录,将程序jar包
pipeline-1.0.jar上传到安装目录,将项目bin目录下的3个启动脚本上传到bin目录,将项目所需要的第三方jar包上传到lib
目录,包括hbase客户端、hive客户端、kafka客户端等所需要的jar包,最后再在conf目录下配好pipeline.properties文件即可。
# 启动脚本
1. 执行bin/start-canal2kafka.sh脚本启动同步canal数据到kafka的MR程序
2. 执行bin/start-streaming.sh脚本启动同步kafka数据到hbase的流处理程序
3. 执行bin/start-kafka2hive.sh脚本启动同步kafka数据到hive的MR程序