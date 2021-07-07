# Partitions
-- #######   store compress and drop partition
-- usage: @partition-interval-drop.sql OWNER TABLE NUMBER_OF_PARTITIONS_TO_KEEP
-- INTERVAL_TYPE INTERVAL_NUMBER
-- example: 6 monthly partitions. @partition-interval-drop.sql IADCLI_IAU
-- IAU_BASE 6 month 1
-- example: 7 daily partitions.  @partition-interval-drop.sql IADCLI_IAU
-- IAU_BASE 7 day 1
-- exaple: 180 daily partitions (~6 months): @partition-interval-drop.sql
-- PRODIAM_IAU OAM 180 day 1

-- TODO
-- 0. add support for multiple partitions drop (12c syntax) via specific flag
-- 1. add flag to allow dropping the first partition,
--      activating the enable/disable drop partition
-- 2. create a shell wrapper to manager input parameters

set serveroutput on
set line 200

-- set ddl_lock_timeout in order to avoid ORA-00054
-- in data 2019.02.05 ho avuto un caso di ORA-00054 anche se ddl_lock_timeout
-- era impostato
alter session set ddl_lock_timeout=600;

declare
    v_statement varchar2(600);
    v_disable_interval varchar2(600);
    v_enable_interval varchar2(600);
    v_count number;
    v_number_of_partitions number;
    v_owner varchar2(30 char) := '&1';
    v_table varchar2(30 char) := '&2';
    v_retention number := '&3';
    v_interval varchar2(30 char) := lower('&4');
    v_interval_integer number := '&5';
    v_interval_function varchar2(30 char);
    v_partition varchar2(30 char);
    v_proceed_flag number;
    -- when set to true, does not drop partitions
    v_dryrun_flag varchar2(30 char) := 'false';
begin
    -- look for table
    select count(*) into v_count from dba_tab_partitions where table_owner=v_owner and table_name=v_table;
    if v_count >= 1 then
        v_proceed_flag := '1';
    else
        v_proceed_flag := '0';
        dbms_output.put_line('ERROR: table does not exist or does not meet partitioning criteria');
    end if;

    -- input validation
    if lower(v_interval) = 'month' then
        v_interval_function := 'numtoyminterval';
        v_proceed_flag := v_proceed_flag + 1;
    elsif lower(v_interval) = 'year' then
        v_interval_function := 'numtoyminterval';
        v_proceed_flag := v_proceed_flag + 1;
    elsif lower(v_interval) = 'day' then
        v_interval_function := 'numtodsinterval';
        v_proceed_flag := v_proceed_flag + 1;
    else
        dbms_output.put_line('ERROR: interval not recognized');
    end if;

    if v_proceed_flag = 2 then
        if v_dryrun_flag = 'true' then
            dbms_output.put_line('INFO: running in dry run mode');
        end if;

        -- disable interval partitioning in order to avoid ORA-14758
        -- v_disable_interval := 'alter table '||v_owner||'.'||v_table||' set
        -- interval ()';
        -- dbms_output.put_line(v_disable_interval);
        -- if v_dryrun_flag = 'false' then
        --     execute immediate v_disable_interval;
        -- end if;

        -- get number of partitions
        select max(partition_position) into v_number_of_partitions from dba_tab_partitions where table_owner=v_owner and table_name=v_table;

        -- select all the partitions which can be dropped, except the first one
        -- (in order to avoid to disable/enable interval partitioning)
        for cc in (select partition_name from dba_tab_partitions where table_owner=v_owner and table_name=v_table and partition_position < (v_number_of_partitions - v_retention) and partition_position != 1) loop
            v_statement := 'alter table '||v_owner||'.'||v_table||' drop partition ' || cc.partition_name || ' update indexes';
            dbms_output.put_line(v_statement);
            if v_dryrun_flag = 'false' then
                execute immediate v_statement;
            end if;
        end loop;

        -- enable interval partitioning
        -- v_enable_interval := 'alter table '||v_owner||'.'||v_table||' set
        -- interval
        -- ('||v_interval_function||'('||v_interval_integer||','''||v_interval||'''))';
        -- dbms_output.put_line(v_enable_interval);
        -- if v_dryrun_flag = 'false' then
        --     execute immediate v_enable_interval;
        -- end if;
    end if;
end;
/

