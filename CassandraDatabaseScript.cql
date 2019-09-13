DROP KEYSPACE IF EXISTS dfanalyzer;
CREATE KEYSPACE dfanalyzer WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE dfanalyzer."Execution" (
	id uuid,
	"ApplicationName" text,
	"EndTime" timestamp,
	"StartTime" timestamp,
	PRIMARY KEY (id)
);

CREATE TABLE dfanalyzer."TransformationGroup" (
	"executionID" uuid,
	id uuid,
  "initTasksIDS" SET<uuid>,
  "intermediaryTasksIDS"  SET<uuid>,
	"finishTaskID" uuid,
	PRIMARY KEY (("executionID"), id)
);

CREATE TABLE dfanalyzer."Task" (
	"executionID" uuid,
	id uuid,
	description text,
	"transformationType" text,
	"usingDefaultSchema" boolean,
	"schemaFields" LIST<text>,
  "hasDataInRepository" boolean,
	PRIMARY KEY (("executionID"), id)
);

CREATE TABLE dfanalyzer."DataElement" (
	"executionID" uuid,
	id uuid,
	values LIST<frozen<LIST<text>>>,
	PRIMARY KEY (("executionID"), id)
);


CREATE TABLE dfanalyzer."FileGroupReference" (
	"executionID" uuid,
	id uuid,// DataElement ID
	"folderPath" text,
	PRIMARY KEY (("executionID"), id)
);

CREATE TABLE dfanalyzer."DependenciesOfTask" (
	"executionID" uuid,
	target uuid,
	source SET<uuid>,
	PRIMARY KEY (("executionID"), target)
);

CREATE TABLE dfanalyzer."DependenciesOfDataElement" (
	"executionID" uuid,
	task uuid,
	source SET<uuid>,
	target uuid,
	PRIMARY KEY (("executionID"), task, target)
);

TRUNCATE dfanalyzer."DataElement";
TRUNCATE dfanalyzer."DependenciesOfDataElement";
TRUNCATE dfanalyzer."Execution";
TRUNCATE dfanalyzer."Task";
TRUNCATE dfanalyzer."DependenciesOfTask";
TRUNCATE dfanalyzer."TransformationGroup";