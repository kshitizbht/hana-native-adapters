## Real time Test Adapter

This adapter is based on SDI AdapterCDC interface to show how we can create an adapter that can push changes to HANA.

After installation of the agent and the default demo adapter was created from the plugin development wizard. 

Subscription class is per table and it maintain all the data required like SDI headers, queue, subscription specification.

### Usage
```
--Let's first create an Agent. 
--drop agent "MyTestAgent" cascade;
create agent "MyTestAgent" protocol 'TCP' host '<hostName>' port 5050;

--Now let's create adapter. 
--drop adapter "SampleAdapter" cascade;
create adapter "SampleAdapter" at location agent "MyTestAgent";
--If you make any changes to the code, please run the following so the server gets the new changes like capabilties and remote source description
alter adapter "SampleAdapter" refresh at location agent "MyTestAgent";

--Create Remote Source via UI. But since this does not have much input we will use SQL
drop remote source "testAdapter" cascade;
CREATE REMOTE SOURCE "testAdapter" ADAPTER "SampleAdapter" AT  LOCATION AGENT "MyTestAgent"  CONFIGURATION 
'<?xml version="1.0" encoding="UTF-8"?>
<ConnectionProperties name="testParam">
       <PropertyEntry name="name">GitUser</PropertyEntry>
</ConnectionProperties>'
WITH CREDENTIAL TYPE 'PASSWORD' USING
'<CredentialEntry name="credential">
		<username>DummyUser</username>
		<password>P455w0rd</password>
</CredentialEntry>';

--Let's create a virtual table
CREATE VIRTUAL TABLE "SYSTEM"."table1" at "testAdapter"."<NULL>"."<NULL>"."RealTimeDemoTable";
drop table T_TABLE;
create table T_TABLE like "SYSTEM"."table1";

--Now real time triggers
--Prior to running any Real time commands like QUEUE/DISTRIBUTE/SUSPEND/CAPTURE.
--Ensure there are not exception on the exception table.
select * from "PUBLIC"."REMOTE_SUBSCRIPTION_EXCEPTIONS"; --check here for exceptions prior to any QUEUE/DISTRIBUTE command
--The above should be clean regarding to this remote source.
--Now let's create triggers
drop remote subscription "TRIGGER";
create remote subscription "TRIGGER" on "SYSTEM"."table1" target table "T_TABLE";
alter remote subscription "TRIGGER" QUEUE;
--Initial load if needed.
alter remote subscription "TRIGGER" DISTRIBUTE;
select * From "SYSTEM"."table1";
select * from T_TABLE;
alter remote subscription "TRIGGER" RESET;


--Let's add more complexity by creating another table 
CREATE VIRTUAL TABLE "SYSTEM"."table2" at "testAdapter"."<NULL>"."<NULL>"."RealTimeDemoTable2";
drop table T_TABLE2;
create table T_TABLE2 like "SYSTEM"."table2";
drop remote subscription "TRIGGER2";
create remote subscription "TRIGGER2" on "SYSTEM"."table2" target table "T_TABLE2";
alter remote subscription "TRIGGER2" QUEUE;
--Initial load if needed.
alter remote subscription "TRIGGER2" DISTRIBUTE;
select * From "SYSTEM"."table2";
select * from T_TABLE2;
alter remote subscription "TRIGGER2" RESET;

--For exception handling. You can ignore or retry.
select * from "PUBLIC"."REMOTE_SUBSCRIPTION_EXCEPTIONS"; --check here for exceptions prior to any QUEUE/DISTRIBUTE command
process remote subscription exception 3 retry;

--For pausing replication say temporary restart/update/upgrade of source 
alter REMOTE SOURCE "testAdapter" suspend capture;

--For resuming after suspend
alter REMOTE SOURCE "testAdapter" resume capture;

--For viewing progress. You should ideally install the Monitioring UI and use RepTask from WebIDE. But at bare minimum during development. You should look at the following,
select * from m_remote_subscriptions; --Subscription information.
select * from remote_subscriptions;
select * from M_REMOTE_SUBSCRIPTION_STATISTICS; --When was last data received from adapter
SELECT * from M_Remote_source_Statistics; --What is adapter doing currently? updated every 5 minutes.
```