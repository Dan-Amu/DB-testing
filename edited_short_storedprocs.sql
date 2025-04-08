ROUTINE_NAME
ROUTINE_DEFINITION
StressTest
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE j INT DEFAULT 0;
    
    -- Create a test table if not exists
    CREATE TABLE IF NOT EXISTS stress_test (
        id INT AUTO_INCREMENT PRIMARY KEY,
        random_data VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Insert operations
    WHILE i < num_operations DO
        SET j = 0;
        START TRANSACTION;
        WHILE j < batch_size DO
            INSERT INTO stress_test (random_data)
            VALUES (CONCAT('Data ', RAND() * 100));
            SET j = j + 1;
        END WHILE;
        COMMIT;
        SET i = i + batch_size;
    END WHILE;
    
    -- Update operations
    SET i = 0;
    WHILE i < num_operations DO
        UPDATE stress_test
        SET random_data = CONCAT(random_data, ' updated')
        ORDER BY RAND()
        LIMIT batch_size;
        SET i = i + batch_size;
    END WHILE;
    
    -- Delete operations
    SET i = 0;
    WHILE i < num_operations DO
        DELETE FROM stress_test
        ORDER BY RAND()
        LIMIT batch_size;
        SET i = i + batch_size;
    END WHILE;
END
StressTestNoDel
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE j INT DEFAULT 0;
    
    -- Create a test table if not exists
    CREATE TABLE IF NOT EXISTS stress_test (
        id INT AUTO_INCREMENT PRIMARY KEY,
        random_data VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Insert operations
    WHILE i < num_operations DO
        SET j = 0;
        START TRANSACTION;
        WHILE j < batch_size DO
            INSERT INTO stress_test (random_data)
            VALUES (CONCAT('Data ', RAND() * 100));
            SET j = j + 1;
        END WHILE;
        COMMIT;
        SET i = i + batch_size;
    END WHILE;
    
    -- Update operations
    SET i = 0;
    WHILE i < num_operations DO
        UPDATE stress_test
        SET random_data = CONCAT(random_data, ' updated')
        ORDER BY RAND()
        LIMIT batch_size;
        SET i = i + batch_size;
    END WHILE;
    
END
AddGeometryColumn
begin
  set @qwe= concat('ALTER TABLE ', t_schema, '.', t_name, ' ADD ', geometry_column,' GEOMETRY REF_SYSTEM_ID=', t_srid); PREPARE ls from @qwe; execute ls; deallocate prepare ls; end
DropGeometryColumn
begin
  set @qwe= concat('ALTER TABLE ', t_schema, '.', t_name, ' DROP ', geometry_column); PREPARE ls from @qwe; execute ls; deallocate prepare ls; end
create_synonym_db
BEGIN
    DECLARE v_done bool DEFAULT FALSE;
    DECLARE v_db_name_check VARCHAR(64);
    DECLARE v_db_err_msg TEXT;
    DECLARE v_table VARCHAR(64);
    DECLARE v_views_created INT DEFAULT 0;
    DECLARE v_table_exists ENUM('', 'BASE TABLE', 'VIEW', 'TEMPORARY') DEFAULT '';
    DECLARE db_doesnt_exist CONDITION FOR SQLSTATE '42000';
    DECLARE db_name_exists CONDITION FOR SQLSTATE 'HY000';
    DECLARE c_table_names CURSOR FOR
        SELECT TABLE_NAME
          FROM INFORMATION_SCHEMA.TABLES
         WHERE TABLE_SCHEMA = in_db_name;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;
    SELECT SCHEMA_NAME INTO v_db_name_check
      FROM INFORMATION_SCHEMA.SCHEMATA
     WHERE SCHEMA_NAME = in_db_name;
    IF v_db_name_check IS NULL THEN
        SET v_db_err_msg = CONCAT('Unknown database ', in_db_name);
        SIGNAL SQLSTATE 'HY000'
            SET MESSAGE_TEXT = v_db_err_msg;
    END IF;
    SELECT SCHEMA_NAME INTO v_db_name_check
      FROM INFORMATION_SCHEMA.SCHEMATA
     WHERE SCHEMA_NAME = in_synonym;
    IF v_db_name_check = in_synonym THEN
        SET v_db_err_msg = CONCAT('Can''t create database ', in_synonym, '; database exists');
        SIGNAL SQLSTATE 'HY000'
            SET MESSAGE_TEXT = v_db_err_msg;
    END IF;
    SET @create_db_stmt := CONCAT('CREATE DATABASE ', sys.quote_identifier(in_synonym));
    PREPARE create_db_stmt FROM @create_db_stmt;
    EXECUTE create_db_stmt;
    DEALLOCATE PREPARE create_db_stmt;
    SET v_done = FALSE;
    OPEN c_table_names;
    c_table_names: LOOP
        FETCH c_table_names INTO v_table;
        IF v_done THEN
            LEAVE c_table_names;
        END IF;
        CALL sys.table_exists(in_db_name, v_table, v_table_exists);
        IF (v_table_exists <> 'TEMPORARY') THEN
            SET @create_view_stmt = CONCAT(
                'CREATE SQL SECURITY INVOKER VIEW ',
                sys.quote_identifier(in_synonym),
                '.',
                sys.quote_identifier(v_table),
                ' AS SELECT * FROM ',
                sys.quote_identifier(in_db_name),
                '.',
                sys.quote_identifier(v_table)
            );
            PREPARE create_view_stmt FROM @create_view_stmt;
            EXECUTE create_view_stmt;
            DEALLOCATE PREPARE create_view_stmt;
            SET v_views_created = v_views_created + 1;
        END IF;
    END LOOP;
    CLOSE c_table_names;
    SELECT CONCAT(
        'Created ', v_views_created, ' view',
        IF(v_views_created != 1, 's', ''), ' in the ',
        sys.quote_identifier(in_synonym), ' database'
    ) AS summary;
END
diagnostics
BEGIN
    DECLARE v_start, v_runtime, v_iter_start, v_sleep DECIMAL(20,2) DEFAULT 0.0;
    DECLARE v_has_innodb, v_has_ndb, v_has_ps, v_has_replication, v_has_ps_replication VARCHAR(8) CHARSET utf8 DEFAULT 'NO';
    DECLARE v_this_thread_enabled, v_has_ps_vars, v_has_metrics ENUM('YES', 'NO');
    DECLARE v_table_name, v_banner VARCHAR(64) CHARSET utf8;
    DECLARE v_sql_status_summary_select, v_sql_status_summary_delta, v_sql_status_summary_from, v_no_delta_names TEXT;
    DECLARE v_output_time, v_output_time_prev DECIMAL(20,3) UNSIGNED;
    DECLARE v_output_count, v_count, v_old_group_concat_max_len INT UNSIGNED DEFAULT 0;
    DECLARE v_status_summary_width TINYINT UNSIGNED DEFAULT 50;
    DECLARE v_done BOOLEAN DEFAULT FALSE;
    DECLARE c_ndbinfo CURSOR FOR
        SELECT TABLE_NAME
          FROM information_schema.TABLES
         WHERE TABLE_SCHEMA = 'ndbinfo'
               AND TABLE_NAME NOT IN (
                  'blocks',
                  'config_params',
                  'dict_obj_types',
                  'disk_write_speed_base',
                  'memory_per_fragment',
                  'memoryusage',
                  'operations_per_fragment',
                  'threadblocks'
               );
    DECLARE c_sysviews_w_delta CURSOR FOR
        SELECT table_name
          FROM tmp_sys_views_delta
         ORDER BY table_name;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;
    SELECT INSTRUMENTED INTO v_this_thread_enabled FROM performance_schema.threads WHERE PROCESSLIST_ID = CONNECTION_ID();
    IF (v_this_thread_enabled = 'YES') THEN
        CALL sys.ps_setup_disable_thread(CONNECTION_ID());
    END IF;
    IF (in_max_runtime < in_interval) THEN
        SIGNAL SQLSTATE '45000'
           SET MESSAGE_TEXT = 'in_max_runtime must be greater than or equal to in_interval';
    END IF;
    IF (in_max_runtime = 0) THEN
        SIGNAL SQLSTATE '45000'
           SET MESSAGE_TEXT = 'in_max_runtime must be greater than 0';
    END IF;
    IF (in_interval = 0) THEN
        SIGNAL SQLSTATE '45000'
           SET MESSAGE_TEXT = 'in_interval must be greater than 0';
    END IF;
    IF (@sys.diagnostics.allow_i_s_tables IS NULL) THEN
        SET @sys.diagnostics.allow_i_s_tables = sys.sys_get_config('diagnostics.allow_i_s_tables', 'OFF');
    END IF;
    IF (@sys.diagnostics.include_raw IS NULL) THEN
        SET @sys.diagnostics.include_raw      = sys.sys_get_config('diagnostics.include_raw'     , 'OFF');
    END IF;
    IF (@sys.debug IS NULL) THEN
        SET @sys.debug                        = sys.sys_get_config('debug'                       , 'OFF');
    END IF;
    IF (@sys.statement_truncate_len IS NULL) THEN
        SET @sys.statement_truncate_len       = sys.sys_get_config('statement_truncate_len'      , '64' );
    END IF;
    SET @log_bin := @@sql_log_bin;
    IF (@log_bin = 1) THEN
        SET sql_log_bin = 0;
    END IF;
    SET v_no_delta_names = CONCAT('s%{COUNT}.Variable_name NOT IN (',
        '''innodb_buffer_pool_pages_total'', ',
        '''innodb_page_size'', ',
        '''last_query_cost'', ',
        '''last_query_partial_plans'', ',
        '''qcache_total_blocks'', ',
        '''slave_last_heartbeat'', ',
        '''ssl_ctx_verify_depth'', ',
        '''ssl_ctx_verify_mode'', ',
        '''ssl_session_cache_size'', ',
        '''ssl_verify_depth'', ',
        '''ssl_verify_mode'', ',
        '''ssl_version'', ',
        '''buffer_flush_lsn_avg_rate'', ',
        '''buffer_flush_pct_for_dirty'', ',
        '''buffer_flush_pct_for_lsn'', ',
        '''buffer_pool_pages_total'', ',
        '''lock_row_lock_time_avg'', ',
        '''lock_row_lock_time_max'', ',
        '''innodb_page_size''',
    ')');
    IF (in_auto_config <> 'current') THEN
        IF (@sys.debug = 'ON') THEN
            SELECT CONCAT('Updating Performance Schema configuration to ', in_auto_config) AS 'Debug';
        END IF;
        CALL sys.ps_setup_save(0);
        IF (in_auto_config = 'medium') THEN
            UPDATE performance_schema.setup_consumers
                SET ENABLED = 'YES'
            WHERE NAME NOT LIKE '%\\_history%';
            UPDATE performance_schema.setup_instruments
                SET ENABLED = 'YES',
                    TIMED   = 'YES'
            WHERE NAME NOT LIKE 'wait/synch/%';
        ELSEIF (in_auto_config = 'full') THEN
            UPDATE performance_schema.setup_consumers
                SET ENABLED = 'YES';
            UPDATE performance_schema.setup_instruments
                SET ENABLED = 'YES',
                    TIMED   = 'YES';
        END IF;
        UPDATE performance_schema.threads
           SET INSTRUMENTED = 'YES'
         WHERE PROCESSLIST_ID <> CONNECTION_ID();
    END IF;
    SET v_start        = UNIX_TIMESTAMP(NOW(2)),
        in_interval    = IFNULL(in_interval, 30),
        in_max_runtime = IFNULL(in_max_runtime, 60);
    SET v_banner = REPEAT(
                      '-',
                      LEAST(
                         GREATEST(
                            36,
                            CHAR_LENGTH(VERSION()),
                            CHAR_LENGTH(@@global.version_comment),
                            CHAR_LENGTH(@@global.version_compile_os),
                            CHAR_LENGTH(@@global.version_compile_machine),
                            CHAR_LENGTH(@@global.socket),
                            CHAR_LENGTH(@@global.datadir)
                         ),
                         64
                      )
                   );
    SELECT 'Hostname' AS 'Name', @@global.hostname AS 'Value'
    UNION ALL
    SELECT 'Port' AS 'Name', @@global.port AS 'Value'
    UNION ALL
    SELECT 'Socket' AS 'Name', @@global.socket AS 'Value'
    UNION ALL
    SELECT 'Datadir' AS 'Name', @@global.datadir AS 'Value'
    UNION ALL
    SELECT REPEAT('-', 23) AS 'Name', v_banner AS 'Value'
    UNION ALL
    SELECT 'MySQL Version' AS 'Name', VERSION() AS 'Value'
    UNION ALL
    SELECT 'Sys Schema Version' AS 'Name', (SELECT sys_version FROM sys.version) AS 'Value'
    UNION ALL
    SELECT 'Version Comment' AS 'Name', @@global.version_comment AS 'Value'
    UNION ALL
    SELECT 'Version Compile OS' AS 'Name', @@global.version_compile_os AS 'Value'
    UNION ALL
    SELECT 'Version Compile Machine' AS 'Name', @@global.version_compile_machine AS 'Value'
    UNION ALL
    SELECT REPEAT('-', 23) AS 'Name', v_banner AS 'Value'
    UNION ALL
    SELECT 'UTC Time' AS 'Name', UTC_TIMESTAMP() AS 'Value'
    UNION ALL
    SELECT 'Local Time' AS 'Name', NOW() AS 'Value'
    UNION ALL
    SELECT 'Time Zone' AS 'Name', @@global.time_zone AS 'Value'
    UNION ALL
    SELECT 'System Time Zone' AS 'Name', @@global.system_time_zone AS 'Value'
    UNION ALL
    SELECT 'Time Zone Offset' AS 'Name', TIMEDIFF(NOW(), UTC_TIMESTAMP()) AS 'Value';
    SET v_has_innodb         = IFNULL((SELECT SUPPORT FROM information_schema.ENGINES WHERE ENGINE = 'InnoDB'), 'NO'),
        v_has_ndb            = IFNULL((SELECT SUPPORT FROM information_schema.ENGINES WHERE ENGINE = 'NDBCluster'), 'NO'),
        v_has_ps             = IFNULL((SELECT SUPPORT FROM information_schema.ENGINES WHERE ENGINE = 'PERFORMANCE_SCHEMA'), 'NO'),
        v_has_ps_replication = IF(v_has_ps = 'YES'
                                   AND EXISTS(SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'performance_schema' AND TABLE_NAME = 'replication_applier_status'),
                                   'YES',
                                   'NO'
                               ),
        v_has_replication    = 'MAYBE',
        v_has_metrics        = IF(v_has_ps = 'YES' OR (sys.version_major() = 5 AND sys.version_minor() = 6), 'YES', 'NO'),
        v_has_ps_vars        = 'NO';
    
    
    IF (@sys.debug = 'ON') THEN
       SELECT v_has_innodb AS 'Has_InnoDB', v_has_ndb AS 'Has_NDBCluster',
              v_has_ps AS 'Has_Performance_Schema', v_has_ps_vars AS 'Has_P_S_SHOW_Variables',
              v_has_metrics AS 'Has_metrics',
              v_has_ps_replication 'AS Has_P_S_Replication', v_has_replication AS 'Has_Replication';
    END IF;
    IF (v_has_innodb IN ('DEFAULT', 'YES')) THEN
        SET @sys.diagnostics.sql = 'SHOW ENGINE InnoDB STATUS';
        PREPARE stmt_innodb_status FROM @sys.diagnostics.sql;
    END IF;
    IF (v_has_ps = 'YES') THEN
        SET @sys.diagnostics.sql = 'SHOW ENGINE PERFORMANCE_SCHEMA STATUS';
        PREPARE stmt_ps_status FROM @sys.diagnostics.sql;
    END IF;
    IF (v_has_ndb IN ('DEFAULT', 'YES')) THEN
        SET @sys.diagnostics.sql = 'SHOW ENGINE NDBCLUSTER STATUS';
        PREPARE stmt_ndbcluster_status FROM @sys.diagnostics.sql;
    END IF;
    SET @sys.diagnostics.sql_gen_query_template = 'SELECT CONCAT(\
           ''SELECT '',\
           GROUP_CONCAT(\
               CASE WHEN (SUBSTRING(TABLE_NAME, 3), COLUMN_NAME) IN (\
                                (''io_global_by_file_by_bytes'', ''total''),\
                                (''io_global_by_wait_by_bytes'', ''total_requested'')\
                         )\
                         THEN CONCAT(''sys.format_bytes('', COLUMN_NAME, '') AS '', COLUMN_NAME)\
                    WHEN SUBSTRING(COLUMN_NAME, -8) = ''_latency''\
                         THEN CONCAT(''format_pico_time('', COLUMN_NAME, '') AS '', COLUMN_NAME)\
                    WHEN SUBSTRING(COLUMN_NAME, -7) = ''_memory'' OR SUBSTRING(COLUMN_NAME, -17) = ''_memory_allocated''\
                         OR ((SUBSTRING(COLUMN_NAME, -5) = ''_read'' OR SUBSTRING(COLUMN_NAME, -8) = ''_written'' OR SUBSTRING(COLUMN_NAME, -6) = ''_write'') AND SUBSTRING(COLUMN_NAME, 1, 6) <> ''COUNT_'')\
                         THEN CONCAT(''sys.format_bytes('', COLUMN_NAME, '') AS '', COLUMN_NAME)\
                    ELSE COLUMN_NAME\
               END\
               ORDER BY ORDINAL_POSITION\
               SEPARATOR '',\
       ''\
           ),\
           ''\
  FROM tmp_'', SUBSTRING(TABLE_NAME FROM 3), ''_%{OUTPUT}''\
       ) AS Query INTO @sys.diagnostics.sql_select\
  FROM information_schema.COLUMNS\
 WHERE TABLE_SCHEMA = ''sys'' AND TABLE_NAME = ?\
 GROUP BY TABLE_NAME';
    SET @sys.diagnostics.sql_gen_query_delta = 'SELECT CONCAT(\
           ''SELECT '',\
           GROUP_CONCAT(\
               CASE WHEN FIND_IN_SET(COLUMN_NAME, diag.pk)\
                         THEN COLUMN_NAME\
                    WHEN diag.TABLE_NAME = ''io_global_by_file_by_bytes'' AND COLUMN_NAME = ''write_pct''\
                         THEN CONCAT(''IFNULL(ROUND(100-(((e.total_read-IFNULL(s.total_read, 0))'',\
                                     ''/NULLIF(((e.total_read-IFNULL(s.total_read, 0))+(e.total_written-IFNULL(s.total_written, 0))), 0))*100), 2), 0.00) AS '',\
                                     COLUMN_NAME)\
                    WHEN (diag.TABLE_NAME, COLUMN_NAME) IN (\
                                (''io_global_by_file_by_bytes'', ''total''),\
                                (''io_global_by_wait_by_bytes'', ''total_requested'')\
                         )\
                         THEN CONCAT(''sys.format_bytes(e.'', COLUMN_NAME, ''-IFNULL(s.'', COLUMN_NAME, '', 0)) AS '', COLUMN_NAME)\
                    WHEN SUBSTRING(COLUMN_NAME, 1, 4) IN (''max_'', ''min_'') AND SUBSTRING(COLUMN_NAME, -8) = ''_latency''\
                         THEN CONCAT(''format_pico_time(e.'', COLUMN_NAME, '') AS '', COLUMN_NAME)\
                    WHEN COLUMN_NAME = ''avg_latency''\
                         THEN CONCAT(''format_pico_time((e.total_latency - IFNULL(s.total_latency, 0))'',\
                                     ''/NULLIF(e.total - IFNULL(s.total, 0), 0)) AS '', COLUMN_NAME)\
                    WHEN SUBSTRING(COLUMN_NAME, -12) = ''_avg_latency''\
                         THEN CONCAT(''format_pico_time((e.'', SUBSTRING(COLUMN_NAME FROM 1 FOR CHAR_LENGTH(COLUMN_NAME)-12), ''_latency - IFNULL(s.'', SUBSTRING(COLUMN_NAME FROM 1 FOR CHAR_LENGTH(COLUMN_NAME)-12), ''_latency, 0))'',\
                                     ''/NULLIF(e.'', SUBSTRING(COLUMN_NAME FROM 1 FOR CHAR_LENGTH(COLUMN_NAME)-12), ''s - IFNULL(s.'', SUBSTRING(COLUMN_NAME FROM 1 FOR CHAR_LENGTH(COLUMN_NAME)-12), ''s, 0), 0)) AS '', COLUMN_NAME)\
                    WHEN SUBSTRING(COLUMN_NAME, -8) = ''_latency''\
                         THEN CONCAT(''format_pico_time(e.'', COLUMN_NAME, '' - IFNULL(s.'', COLUMN_NAME, '', 0)) AS '', COLUMN_NAME)\
                    WHEN COLUMN_NAME IN (''avg_read'', ''avg_write'', ''avg_written'')\
                         THEN CONCAT(''sys.format_bytes(IFNULL((e.total_'', IF(COLUMN_NAME = ''avg_read'', ''read'', ''written''), ''-IFNULL(s.total_'', IF(COLUMN_NAME = ''avg_read'', ''read'', ''written''), '', 0))'',\
                                     ''/NULLIF(e.count_'', IF(COLUMN_NAME = ''avg_read'', ''read'', ''write''), ''-IFNULL(s.count_'', IF(COLUMN_NAME = ''avg_read'', ''read'', ''write''), '', 0), 0), 0)) AS '',\
                                     COLUMN_NAME)\
                    WHEN SUBSTRING(COLUMN_NAME, -7) = ''_memory'' OR SUBSTRING(COLUMN_NAME, -17) = ''_memory_allocated''\
                         OR ((SUBSTRING(COLUMN_NAME, -5) = ''_read'' OR SUBSTRING(COLUMN_NAME, -8) = ''_written'' OR SUBSTRING(COLUMN_NAME, -6) = ''_write'') AND SUBSTRING(COLUMN_NAME, 1, 6) <> ''COUNT_'')\
                         THEN CONCAT(''sys.format_bytes(e.'', COLUMN_NAME, '' - IFNULL(s.'', COLUMN_NAME, '', 0)) AS '', COLUMN_NAME)\
                    ELSE CONCAT(''(e.'', COLUMN_NAME, '' - IFNULL(s.'', COLUMN_NAME, '', 0)) AS '', COLUMN_NAME)\
               END\
               ORDER BY ORDINAL_POSITION\
               SEPARATOR '',\
       ''\
           ),\
           ''\
  FROM tmp_'', diag.TABLE_NAME, ''_end e\
       LEFT OUTER JOIN tmp_'', diag.TABLE_NAME, ''_start s USING ('', diag.pk, '')''\
       ) AS Query INTO @sys.diagnostics.sql_select\
  FROM tmp_sys_views_delta diag\
       INNER JOIN information_schema.COLUMNS c ON c.TABLE_NAME = CONCAT(''x$'', diag.TABLE_NAME)\
 WHERE c.TABLE_SCHEMA = ''sys'' AND diag.TABLE_NAME = ?\
 GROUP BY diag.TABLE_NAME';
    IF (v_has_ps = 'YES') THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_sys_views_delta;
        CREATE TEMPORARY TABLE tmp_sys_views_delta (
            TABLE_NAME varchar(64) NOT NULL,
            order_by text COMMENT 'ORDER BY clause for the initial and overall views',
            order_by_delta text COMMENT 'ORDER BY clause for the delta views',
            where_delta text COMMENT 'WHERE clause to use for delta views to only include rows with a "count" > 0',
            limit_rows int unsigned COMMENT 'The maximum number of rows to include for the view',
            pk varchar(128) COMMENT 'Used with the FIND_IN_SET() function so use comma separated list without whitespace',
            PRIMARY KEY (TABLE_NAME)
        );
        IF (@sys.debug = 'ON') THEN
            SELECT 'Populating tmp_sys_views_delta' AS 'Debug';
        END IF;
        INSERT INTO tmp_sys_views_delta
        VALUES ('host_summary'                       , '%{TABLE}.statement_latency DESC',
                                                       '(e.statement_latency-IFNULL(s.statement_latency, 0)) DESC',
                                                       '(e.statements - IFNULL(s.statements, 0)) > 0', NULL, 'host'),
               ('host_summary_by_file_io'            , '%{TABLE}.io_latency DESC',
                                                       '(e.io_latency-IFNULL(s.io_latency, 0)) DESC',
                                                       '(e.ios - IFNULL(s.ios, 0)) > 0', NULL, 'host'),
               ('host_summary_by_file_io_type'       , '%{TABLE}.host, %{TABLE}.total_latency DESC',
                                                       'e.host, (e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'host,event_name'),
               ('host_summary_by_stages'             , '%{TABLE}.host, %{TABLE}.total_latency DESC',
                                                       'e.host, (e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'host,event_name'),
               ('host_summary_by_statement_latency'  , '%{TABLE}.total_latency DESC',
                                                       '(e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'host'),
               ('host_summary_by_statement_type'     , '%{TABLE}.host, %{TABLE}.total_latency DESC',
                                                       'e.host, (e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'host,statement'),
               ('io_by_thread_by_latency'            , '%{TABLE}.total_latency DESC',
                                                       '(e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'user,thread_id,processlist_id'),
               ('io_global_by_file_by_bytes'         , '%{TABLE}.total DESC',
                                                       '(e.total-IFNULL(s.total, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', 100, 'file'),
               ('io_global_by_file_by_latency'       , '%{TABLE}.total_latency DESC',
                                                       '(e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', 100, 'file'),
               ('io_global_by_wait_by_bytes'         , '%{TABLE}.total_requested DESC',
                                                       '(e.total_requested-IFNULL(s.total_requested, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'event_name'),
               ('io_global_by_wait_by_latency'       , '%{TABLE}.total_latency DESC',
                                                       '(e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'event_name'),
               ('schema_index_statistics'            , '(%{TABLE}.select_latency+%{TABLE}.insert_latency+%{TABLE}.update_latency+%{TABLE}.delete_latency) DESC',
                                                       '((e.select_latency+e.insert_latency+e.update_latency+e.delete_latency)-IFNULL(s.select_latency+s.insert_latency+s.update_latency+s.delete_latency, 0)) DESC',
                                                       '((e.rows_selected+e.insert_latency+e.rows_updated+e.rows_deleted)-IFNULL(s.rows_selected+s.rows_inserted+s.rows_updated+s.rows_deleted, 0)) > 0',
                                                       100, 'table_schema,table_name,index_name'),
               ('schema_table_statistics'            , '%{TABLE}.total_latency DESC',
                                                       '(e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total_latency-IFNULL(s.total_latency, 0)) > 0', 100, 'table_schema,table_name'),
               ('schema_tables_with_full_table_scans', '%{TABLE}.rows_full_scanned DESC',
                                                       '(e.rows_full_scanned-IFNULL(s.rows_full_scanned, 0)) DESC',
                                                       '(e.rows_full_scanned-IFNULL(s.rows_full_scanned, 0)) > 0', 100, 'object_schema,object_name'),
               ('user_summary'                       , '%{TABLE}.statement_latency DESC',
                                                       '(e.statement_latency-IFNULL(s.statement_latency, 0)) DESC',
                                                       '(e.statements - IFNULL(s.statements, 0)) > 0', NULL, 'user'),
               ('user_summary_by_file_io'            , '%{TABLE}.io_latency DESC',
                                                       '(e.io_latency-IFNULL(s.io_latency, 0)) DESC',
                                                       '(e.ios - IFNULL(s.ios, 0)) > 0', NULL, 'user'),
               ('user_summary_by_file_io_type'       , '%{TABLE}.user, %{TABLE}.latency DESC',
                                                       'e.user, (e.latency-IFNULL(s.latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'user,event_name'),
               ('user_summary_by_stages'             , '%{TABLE}.user, %{TABLE}.total_latency DESC',
                                                       'e.user, (e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'user,event_name'),
               ('user_summary_by_statement_latency'  , '%{TABLE}.total_latency DESC',
                                                       '(e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'user'),
               ('user_summary_by_statement_type'     , '%{TABLE}.user, %{TABLE}.total_latency DESC',
                                                       'e.user, (e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'user,statement'),
               ('wait_classes_global_by_avg_latency' , 'IFNULL(%{TABLE}.total_latency / NULLIF(%{TABLE}.total, 0), 0) DESC',
                                                       'IFNULL((e.total_latency-IFNULL(s.total_latency, 0)) / NULLIF((e.total - IFNULL(s.total, 0)), 0), 0) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'event_class'),
               ('wait_classes_global_by_latency'     , '%{TABLE}.total_latency DESC',
                                                       '(e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'event_class'),
               ('waits_by_host_by_latency'           , '%{TABLE}.host, %{TABLE}.total_latency DESC',
                                                       'e.host, (e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'host,event'),
               ('waits_by_user_by_latency'           , '%{TABLE}.user, %{TABLE}.total_latency DESC',
                                                       'e.user, (e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'user,event'),
               ('waits_global_by_latency'            , '%{TABLE}.total_latency DESC',
                                                       '(e.total_latency-IFNULL(s.total_latency, 0)) DESC',
                                                       '(e.total - IFNULL(s.total, 0)) > 0', NULL, 'events')
        ;
    END IF;
    SELECT '\
=======================\
     Configuration\
=======================\
' AS '';
    SELECT 'GLOBAL VARIABLES' AS 'The following output is:';
    IF (v_has_ps_vars = 'YES') THEN
        SELECT LOWER(VARIABLE_NAME) AS Variable_name, VARIABLE_VALUE AS Variable_value FROM performance_schema.global_variables ORDER BY VARIABLE_NAME;
    ELSE
        SELECT LOWER(VARIABLE_NAME) AS Variable_name, VARIABLE_VALUE AS Variable_value FROM information_schema.GLOBAL_VARIABLES ORDER BY VARIABLE_NAME;
    END IF;
    IF (v_has_ps = 'YES') THEN
        SELECT 'Performance Schema Setup - Actors' AS 'The following output is:';
        SELECT * FROM performance_schema.setup_actors;
        SELECT 'Performance Schema Setup - Consumers' AS 'The following output is:';
        SELECT NAME AS Consumer, ENABLED, sys.ps_is_consumer_enabled(NAME) AS COLLECTS
          FROM performance_schema.setup_consumers;
        SELECT 'Performance Schema Setup - Instruments' AS 'The following output is:';
        SELECT SUBSTRING_INDEX(NAME, '/', 2) AS 'InstrumentClass',
               ROUND(100*SUM(IF(ENABLED = 'YES', 1, 0))/COUNT(*), 2) AS 'EnabledPct',
               ROUND(100*SUM(IF(TIMED = 'YES', 1, 0))/COUNT(*), 2) AS 'TimedPct'
          FROM performance_schema.setup_instruments
         GROUP BY SUBSTRING_INDEX(NAME, '/', 2)
         ORDER BY SUBSTRING_INDEX(NAME, '/', 2);
        SELECT 'Performance Schema Setup - Objects' AS 'The following output is:';
        SELECT * FROM performance_schema.setup_objects;
        SELECT 'Performance Schema Setup - Threads' AS 'The following output is:';
        SELECT `TYPE` AS ThreadType, COUNT(*) AS 'Total', ROUND(100*SUM(IF(INSTRUMENTED = 'YES', 1, 0))/COUNT(*), 2) AS 'InstrumentedPct'
          FROM performance_schema.threads
        GROUP BY TYPE;
    END IF;
    IF (v_has_replication = 'NO') THEN
        SELECT 'No Replication Configured' AS 'Replication Status';
    ELSE
        SELECT CONCAT('Replication Configured: ', v_has_replication, ' - Performance Schema Replication Tables: ', v_has_ps_replication) AS 'Replication Status';
        IF (v_has_ps_replication = 'YES') THEN
            SELECT 'Replication - Connection Configuration' AS 'The following output is:';
            SELECT * FROM performance_schema.replication_connection_configuration;
        END IF;
        IF (v_has_ps_replication = 'YES') THEN
            SELECT 'Replication - Applier Configuration' AS 'The following output is:';
            SELECT * FROM performance_schema.replication_applier_configuration ORDER BY CHANNEL_NAME;
        END IF;
    END IF;
    IF (v_has_ndb IN ('DEFAULT', 'YES')) THEN
       SELECT 'Cluster Thread Blocks' AS 'The following output is:';
       SELECT * FROM ndbinfo.threadblocks;
    END IF;
    IF (v_has_ps = 'YES') THEN
        IF (@sys.diagnostics.include_raw = 'ON') THEN
            SELECT '\
========================\
     Initial Status\
========================\
' AS '';
        END IF;
        DROP TEMPORARY TABLE IF EXISTS tmp_digests_start;
        CALL sys.statement_performance_analyzer('create_tmp', 'tmp_digests_start', NULL);
        CALL sys.statement_performance_analyzer('snapshot', NULL, NULL);
        CALL sys.statement_performance_analyzer('save', 'tmp_digests_start', NULL);
        IF (@sys.diagnostics.include_raw = 'ON') THEN
            SET @sys.diagnostics.sql = REPLACE(@sys.diagnostics.sql_gen_query_template, '%{OUTPUT}', 'start');
            IF (@sys.debug = 'ON') THEN
                SELECT 'The following query will be used to generate the query for each sys view' AS 'Debug';
                SELECT @sys.diagnostics.sql AS 'Debug';
            END IF;
            PREPARE stmt_gen_query FROM @sys.diagnostics.sql;
        END IF;
        SET v_done = FALSE;
        OPEN c_sysviews_w_delta;
        c_sysviews_w_delta_loop: LOOP
            FETCH c_sysviews_w_delta INTO v_table_name;
            IF v_done THEN
                LEAVE c_sysviews_w_delta_loop;
            END IF;
            IF (@sys.debug = 'ON') THEN
                SELECT CONCAT('The following queries are for storing the initial content of ', v_table_name) AS 'Debug';
            END IF;
            CALL sys.execute_prepared_stmt(CONCAT('DROP TEMPORARY TABLE IF EXISTS `tmp_', v_table_name, '_start`'));
            CALL sys.execute_prepared_stmt(CONCAT('CREATE TEMPORARY TABLE `tmp_', v_table_name, '_start` SELECT * FROM `sys`.`x$', v_table_name, '`'));
            IF (@sys.diagnostics.include_raw = 'ON') THEN
                SET @sys.diagnostics.table_name = CONCAT('x$', v_table_name);
                EXECUTE stmt_gen_query USING @sys.diagnostics.table_name;
                SELECT CONCAT(@sys.diagnostics.sql_select,
                              IF(order_by IS NOT NULL, CONCAT('\
 ORDER BY ', REPLACE(order_by, '%{TABLE}', CONCAT('tmp_', v_table_name, '_start'))), ''),
                              IF(limit_rows IS NOT NULL, CONCAT('\
 LIMIT ', limit_rows), '')
                       )
                  INTO @sys.diagnostics.sql_select
                  FROM tmp_sys_views_delta
                 WHERE TABLE_NAME = v_table_name;
                SELECT CONCAT('Initial ', v_table_name) AS 'The following output is:';
                CALL sys.execute_prepared_stmt(@sys.diagnostics.sql_select);
            END IF;
        END LOOP;
        CLOSE c_sysviews_w_delta;
        IF (@sys.diagnostics.include_raw = 'ON') THEN
            DEALLOCATE PREPARE stmt_gen_query;
        END IF;
    END IF;
    SET v_sql_status_summary_select = 'SELECT Variable_name',
        v_sql_status_summary_delta  = '',
        v_sql_status_summary_from   = '';
    REPEAT
        SET v_output_count = v_output_count + 1;
        IF (v_output_count > 1) THEN
            SET v_sleep = in_interval-(UNIX_TIMESTAMP(NOW(2))-v_iter_start);
            SELECT NOW() AS 'Time', CONCAT('Going to sleep for ', v_sleep, ' seconds. Please do not interrupt') AS 'The following output is:';
            DO SLEEP(in_interval);
        END IF;
        SET v_iter_start = UNIX_TIMESTAMP(NOW(2));
        SELECT NOW(), CONCAT('Iteration Number ', IFNULL(v_output_count, 'NULL')) AS 'The following output is:';
        IF (@@log_bin = 1) THEN
            SELECT 'SHOW MASTER STATUS' AS 'The following output is:';
            SHOW MASTER STATUS;
        END IF;
        IF (v_has_replication <> 'NO') THEN
            SELECT 'SHOW SLAVE STATUS' AS 'The following output is:';
            SHOW SLAVE STATUS;
        END IF;
        SET v_table_name = CONCAT('tmp_metrics_', v_output_count);
        CALL sys.execute_prepared_stmt(CONCAT('DROP TEMPORARY TABLE IF EXISTS ', v_table_name));
        CALL sys.execute_prepared_stmt(CONCAT('CREATE TEMPORARY TABLE ', v_table_name, ' (\
  Variable_name VARCHAR(193) NOT NULL,\
  Variable_value VARCHAR(1024),\
  Type VARCHAR(100) NOT NULL,\
  Enabled ENUM(''YES'', ''NO'', ''PARTIAL'') NOT NULL,\
  PRIMARY KEY (Type, Variable_name)\
) ENGINE = InnoDB DEFAULT CHARSET=utf8'));
        IF (v_has_metrics) THEN
            SET @sys.diagnostics.sql = CONCAT(
                'INSERT INTO ', v_table_name,
                ' SELECT Variable_name, REPLACE(Variable_value, ''\
'', ''\\\
'') AS Variable_value, Type, Enabled FROM sys.metrics'
            );
        ELSE
            SET @sys.diagnostics.sql = CONCAT(
                'INSERT INTO ', v_table_name,
                '(SELECT LOWER(VARIABLE_NAME) AS Variable_name, REPLACE(VARIABLE_VALUE, ''\
'', ''\\\
'') AS Variable_value,\
                         ''Global Status'' AS Type, ''YES'' AS Enabled\
  FROM performance_schema.global_status\
) UNION ALL (\
SELECT NAME AS Variable_name, COUNT AS Variable_value,\
       CONCAT(''InnoDB Metrics - '', SUBSYSTEM) AS Type,\
       IF(STATUS = ''enabled'', ''YES'', ''NO'') AS Enabled\
  FROM information_schema.INNODB_METRICS\
 WHERE NAME NOT IN (\
     ''lock_row_lock_time'', ''lock_row_lock_time_avg'', ''lock_row_lock_time_max'', ''lock_row_lock_waits'',\
     ''buffer_pool_reads'', ''buffer_pool_read_requests'', ''buffer_pool_write_requests'', ''buffer_pool_wait_free'',\
     ''buffer_pool_read_ahead'', ''buffer_pool_read_ahead_evicted'', ''buffer_pool_pages_total'', ''buffer_pool_pages_misc'',\
     ''buffer_pool_pages_data'', ''buffer_pool_bytes_data'', ''buffer_pool_pages_dirty'', ''buffer_pool_bytes_dirty'',\
     ''buffer_pool_pages_free'', ''buffer_pages_created'', ''buffer_pages_written'', ''buffer_pages_read'',\
     ''buffer_data_reads'', ''buffer_data_written'', ''file_num_open_files'',\
     ''os_log_bytes_written'', ''os_log_fsyncs'', ''os_log_pending_fsyncs'', ''os_log_pending_writes'',\
     ''log_waits'', ''log_write_requests'', ''log_writes'', ''innodb_dblwr_writes'', ''innodb_dblwr_pages_written'', ''innodb_page_size'')\
) UNION ALL (\
SELECT ''NOW()'' AS Variable_name, NOW(3) AS Variable_value, ''System Time'' AS Type, ''YES'' AS Enabled\
) UNION ALL (\
SELECT ''UNIX_TIMESTAMP()'' AS Variable_name, ROUND(UNIX_TIMESTAMP(NOW(3)), 3) AS Variable_value, ''System Time'' AS Type, ''YES'' AS Enabled\
)\
 ORDER BY Type, Variable_name;'
            );
        END IF;
        CALL sys.execute_prepared_stmt(@sys.diagnostics.sql);
        CALL sys.execute_prepared_stmt(
            CONCAT('SELECT Variable_value INTO @sys.diagnostics.output_time FROM ', v_table_name, ' WHERE Type = ''System Time'' AND Variable_name = ''UNIX_TIMESTAMP()''')
        );
        SET v_output_time = @sys.diagnostics.output_time;
        SET v_sql_status_summary_select = CONCAT(v_sql_status_summary_select, ',\
       CONCAT(\
           LEFT(s', v_output_count, '.Variable_value, ', v_status_summary_width, '),\
           IF(', REPLACE(v_no_delta_names, '%{COUNT}', v_output_count), ' AND s', v_output_count, '.Variable_value REGEXP ''^[0-9]+(\\\\.[0-9]+)?$'', CONCAT('' ('', ROUND(s', v_output_count, '.Variable_value/', v_output_time, ', 2), ''/sec)''), '''')\
       ) AS ''Output ', v_output_count, ''''),
            v_sql_status_summary_from   = CONCAT(v_sql_status_summary_from, '\
',
                                                    IF(v_output_count = 1, '  FROM ', '       INNER JOIN '),
                                                    v_table_name, ' s', v_output_count,
                                                    IF (v_output_count = 1, '', ' USING (Type, Variable_name)'));
        IF (v_output_count > 1) THEN
            SET v_sql_status_summary_delta  = CONCAT(v_sql_status_summary_delta, ',\
       IF(', REPLACE(v_no_delta_names, '%{COUNT}', v_output_count), ' AND s', (v_output_count-1), '.Variable_value REGEXP ''^[0-9]+(\\\\.[0-9]+)?$'' AND s', v_output_count, '.Variable_value REGEXP ''^[0-9]+(\\\\.[0-9]+)?$'',\
          CONCAT(IF(s', (v_output_count-1), '.Variable_value REGEXP ''^[0-9]+\\\\.[0-9]+$'' OR s', v_output_count, '.Variable_value REGEXP ''^[0-9]+\\\\.[0-9]+$'',\
                    ROUND((s', v_output_count, '.Variable_value-s', (v_output_count-1), '.Variable_value), 2),\
                    (s', v_output_count, '.Variable_value-s', (v_output_count-1), '.Variable_value)\
                   ),\
                 '' ('', ROUND((s', v_output_count, '.Variable_value-s', (v_output_count-1), '.Variable_value)/(', v_output_time, '-', v_output_time_prev, '), 2), ''/sec)''\
          ),\
          ''''\
       ) AS ''Delta (', (v_output_count-1), ' -> ', v_output_count, ')''');
        END IF;
        SET v_output_time_prev = v_output_time;
        IF (@sys.diagnostics.include_raw = 'ON') THEN
            IF (v_has_metrics) THEN
                SELECT 'SELECT * FROM sys.metrics' AS 'The following output is:';
            ELSE
                SELECT 'sys.metrics equivalent' AS 'The following output is:';
            END IF;
            CALL sys.execute_prepared_stmt(CONCAT('SELECT Type, Variable_name, Enabled, Variable_value FROM ', v_table_name, ' ORDER BY Type, Variable_name'));
        END IF;
        IF (v_has_innodb IN ('DEFAULT', 'YES')) THEN
            SELECT 'SHOW ENGINE INNODB STATUS' AS 'The following output is:';
            EXECUTE stmt_innodb_status;
            SELECT 'InnoDB - Transactions' AS 'The following output is:';
            SELECT * FROM information_schema.INNODB_TRX;
        END IF;
        IF (v_has_ndb IN ('DEFAULT', 'YES')) THEN
            SELECT 'SHOW ENGINE NDBCLUSTER STATUS' AS 'The following output is:';
            EXECUTE stmt_ndbcluster_status;
            SELECT 'ndbinfo.memoryusage' AS 'The following output is:';
            SELECT node_id, memory_type, sys.format_bytes(used) AS used, used_pages, sys.format_bytes(total) AS total, total_pages,
                   ROUND(100*(used/total), 2) AS 'Used %'
            FROM ndbinfo.memoryusage;
            SET v_done = FALSE;
            OPEN c_ndbinfo;
            c_ndbinfo_loop: LOOP
                FETCH c_ndbinfo INTO v_table_name;
                IF v_done THEN
                LEAVE c_ndbinfo_loop;
                END IF;
                SELECT CONCAT('SELECT * FROM ndbinfo.', v_table_name) AS 'The following output is:';
                CALL sys.execute_prepared_stmt(CONCAT('SELECT * FROM `ndbinfo`.`', v_table_name, '`'));
            END LOOP;
            CLOSE c_ndbinfo;
            SELECT * FROM information_schema.FILES;
        END IF;
        SELECT 'SELECT * FROM sys.processlist' AS 'The following output is:';
        SELECT processlist.* FROM sys.processlist;
        IF (v_has_ps = 'YES') THEN
            IF (sys.ps_is_consumer_enabled('events_waits_history_long') = 'YES') THEN
                SELECT 'SELECT * FROM sys.latest_file_io' AS 'The following output is:';
                SELECT * FROM sys.latest_file_io;
            END IF;
            IF (EXISTS(SELECT 1 FROM performance_schema.setup_instruments WHERE NAME LIKE 'memory/%' AND ENABLED = 'YES')) THEN
                SELECT 'SELECT * FROM sys.memory_by_host_by_current_bytes' AS 'The following output is:';
                SELECT * FROM sys.memory_by_host_by_current_bytes;
                SELECT 'SELECT * FROM sys.memory_by_thread_by_current_bytes' AS 'The following output is:';
                SELECT * FROM sys.memory_by_thread_by_current_bytes;
                SELECT 'SELECT * FROM sys.memory_by_user_by_current_bytes' AS 'The following output is:';
                SELECT * FROM sys.memory_by_user_by_current_bytes;
                SELECT 'SELECT * FROM sys.memory_global_by_current_bytes' AS 'The following output is:';
                SELECT * FROM sys.memory_global_by_current_bytes;
            END IF;
        END IF;
        SET v_runtime = (UNIX_TIMESTAMP(NOW(2)) - v_start);
    UNTIL (v_runtime + in_interval >= in_max_runtime) END REPEAT;
    IF (v_has_ps = 'YES') THEN
        SELECT 'SHOW ENGINE PERFORMANCE_SCHEMA STATUS' AS 'The following output is:';
        EXECUTE stmt_ps_status;
    END IF;
    IF (v_has_innodb IN ('DEFAULT', 'YES')) THEN
        DEALLOCATE PREPARE stmt_innodb_status;
    END IF;
    IF (v_has_ps = 'YES') THEN
        DEALLOCATE PREPARE stmt_ps_status;
    END IF;
    IF (v_has_ndb IN ('DEFAULT', 'YES')) THEN
        DEALLOCATE PREPARE stmt_ndbcluster_status;
    END IF;
    SELECT '\
============================\
     Schema Information\
============================\
' AS '';
    SELECT COUNT(*) AS 'Total Number of Tables' FROM information_schema.TABLES;
    IF (@sys.diagnostics.allow_i_s_tables = 'ON') THEN
        SELECT 'Storage Engine Usage' AS 'The following output is:';
        SELECT ENGINE, COUNT(*) AS NUM_TABLES,
                sys.format_bytes(SUM(DATA_LENGTH)) AS DATA_LENGTH,
                sys.format_bytes(SUM(INDEX_LENGTH)) AS INDEX_LENGTH,
                sys.format_bytes(SUM(DATA_LENGTH+INDEX_LENGTH)) AS TOTAL
            FROM information_schema.TABLES
            GROUP BY ENGINE;
        SELECT 'Schema Object Overview' AS 'The following output is:';
        SELECT * FROM sys.schema_object_overview;
        SELECT 'Tables without a PRIMARY KEY' AS 'The following output is:';
        SELECT TABLES.TABLE_SCHEMA, ENGINE, COUNT(*) AS NumTables
          FROM information_schema.TABLES
               LEFT OUTER JOIN information_schema.STATISTICS ON STATISTICS.TABLE_SCHEMA = TABLES.TABLE_SCHEMA
                                                                AND STATISTICS.TABLE_NAME = TABLES.TABLE_NAME
                                                                AND STATISTICS.INDEX_NAME = 'PRIMARY'
         WHERE STATISTICS.TABLE_NAME IS NULL
               AND TABLES.TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
               AND TABLES.TABLE_TYPE = 'BASE TABLE'
         GROUP BY TABLES.TABLE_SCHEMA, ENGINE;
    END IF;
    IF (v_has_ps = 'YES') THEN
        SELECT 'Unused Indexes' AS 'The following output is:';
        SELECT object_schema, COUNT(*) AS NumUnusedIndexes
          FROM performance_schema.table_io_waits_summary_by_index_usage
         WHERE index_name IS NOT NULL
               AND count_star = 0
               AND object_schema NOT IN ('mysql', 'sys')
               AND index_name != 'PRIMARY'
         GROUP BY object_schema;
    END IF;
    IF (v_has_ps = 'YES') THEN
        SELECT '\
=========================\
     Overall Status\
=========================\
' AS '';
        SELECT 'CALL sys.ps_statement_avg_latency_histogram()' AS 'The following output is:';
        CALL sys.ps_statement_avg_latency_histogram();
        CALL sys.statement_performance_analyzer('snapshot', NULL, NULL);
        CALL sys.statement_performance_analyzer('overall', NULL, 'with_runtimes_in_95th_percentile');
        SET @sys.diagnostics.sql = REPLACE(@sys.diagnostics.sql_gen_query_template, '%{OUTPUT}', 'end');
        IF (@sys.debug = 'ON') THEN
            SELECT 'The following query will be used to generate the query for each sys view' AS 'Debug';
            SELECT @sys.diagnostics.sql AS 'Debug';
        END IF;
        PREPARE stmt_gen_query FROM @sys.diagnostics.sql;
        SET v_done = FALSE;
        OPEN c_sysviews_w_delta;
        c_sysviews_w_delta_loop: LOOP
            FETCH c_sysviews_w_delta INTO v_table_name;
            IF v_done THEN
                LEAVE c_sysviews_w_delta_loop;
            END IF;
            IF (@sys.debug = 'ON') THEN
                SELECT CONCAT('The following queries are for storing the final content of ', v_table_name) AS 'Debug';
            END IF;
            CALL sys.execute_prepared_stmt(CONCAT('DROP TEMPORARY TABLE IF EXISTS `tmp_', v_table_name, '_end`'));
            CALL sys.execute_prepared_stmt(CONCAT('CREATE TEMPORARY TABLE `tmp_', v_table_name, '_end` SELECT * FROM `sys`.`x$', v_table_name, '`'));
            IF (@sys.diagnostics.include_raw = 'ON') THEN
                SET @sys.diagnostics.table_name = CONCAT('x$', v_table_name);
                EXECUTE stmt_gen_query USING @sys.diagnostics.table_name;
                SELECT CONCAT(@sys.diagnostics.sql_select,
                                IF(order_by IS NOT NULL, CONCAT('\
 ORDER BY ', REPLACE(order_by, '%{TABLE}', CONCAT('tmp_', v_table_name, '_end'))), ''),
                                IF(limit_rows IS NOT NULL, CONCAT('\
 LIMIT ', limit_rows), '')
                        )
                    INTO @sys.diagnostics.sql_select
                    FROM tmp_sys_views_delta
                    WHERE TABLE_NAME = v_table_name;
                SELECT CONCAT('Overall ', v_table_name) AS 'The following output is:';
                CALL sys.execute_prepared_stmt(@sys.diagnostics.sql_select);
            END IF;
        END LOOP;
        CLOSE c_sysviews_w_delta;
        DEALLOCATE PREPARE stmt_gen_query;
        SELECT '\
======================\
     Delta Status\
======================\
' AS '';
        CALL sys.statement_performance_analyzer('delta', 'tmp_digests_start', 'with_runtimes_in_95th_percentile');
        CALL sys.statement_performance_analyzer('cleanup', NULL, NULL);
        DROP TEMPORARY TABLE tmp_digests_start;
        IF (@sys.debug = 'ON') THEN
            SELECT 'The following query will be used to generate the query for each sys view delta' AS 'Debug';
            SELECT @sys.diagnostics.sql_gen_query_delta AS 'Debug';
        END IF;
        PREPARE stmt_gen_query_delta FROM @sys.diagnostics.sql_gen_query_delta;
        SET v_old_group_concat_max_len = @@session.group_concat_max_len;
        SET @@session.group_concat_max_len = 2048;
        SET v_done = FALSE;
        OPEN c_sysviews_w_delta;
        c_sysviews_w_delta_loop: LOOP
            FETCH c_sysviews_w_delta INTO v_table_name;
            IF v_done THEN
                LEAVE c_sysviews_w_delta_loop;
            END IF;
            SET @sys.diagnostics.table_name = v_table_name;
            EXECUTE stmt_gen_query_delta USING @sys.diagnostics.table_name;
            SELECT CONCAT(@sys.diagnostics.sql_select,
                            IF(where_delta IS NOT NULL, CONCAT('\
 WHERE ', where_delta), ''),
                            IF(order_by_delta IS NOT NULL, CONCAT('\
 ORDER BY ', order_by_delta), ''),
                            IF(limit_rows IS NOT NULL, CONCAT('\
 LIMIT ', limit_rows), '')
                    )
                INTO @sys.diagnostics.sql_select
                FROM tmp_sys_views_delta
                WHERE TABLE_NAME = v_table_name;
            SELECT CONCAT('Delta ', v_table_name) AS 'The following output is:';
            CALL sys.execute_prepared_stmt(@sys.diagnostics.sql_select);
            CALL sys.execute_prepared_stmt(CONCAT('DROP TEMPORARY TABLE `tmp_', v_table_name, '_end`'));
            CALL sys.execute_prepared_stmt(CONCAT('DROP TEMPORARY TABLE `tmp_', v_table_name, '_start`'));
        END LOOP;
        CLOSE c_sysviews_w_delta;
        SET @@session.group_concat_max_len = v_old_group_concat_max_len;
        DEALLOCATE PREPARE stmt_gen_query_delta;
        DROP TEMPORARY TABLE tmp_sys_views_delta;
    END IF;
    IF (v_has_metrics) THEN
        SELECT 'SELECT * FROM sys.metrics' AS 'The following output is:';
    ELSE
        SELECT 'sys.metrics equivalent' AS 'The following output is:';
    END IF;
    CALL sys.execute_prepared_stmt(
        CONCAT(v_sql_status_summary_select, v_sql_status_summary_delta, ', Type, s1.Enabled', v_sql_status_summary_from,
               '\
 ORDER BY Type, Variable_name'
        )
    );
    SET v_count = 0;
    WHILE (v_count < v_output_count) DO
        SET v_count = v_count + 1;
        SET v_table_name = CONCAT('tmp_metrics_', v_count);
        CALL sys.execute_prepared_stmt(CONCAT('DROP TEMPORARY TABLE IF EXISTS ', v_table_name));
    END WHILE;
    IF (in_auto_config <> 'current') THEN
        CALL sys.ps_setup_reload_saved();
        SET sql_log_bin = @log_bin;
    END IF;
    SET @sys.diagnostics.output_time            = NULL,
        @sys.diagnostics.sql                    = NULL,
        @sys.diagnostics.sql_gen_query_delta    = NULL,
        @sys.diagnostics.sql_gen_query_template = NULL,
        @sys.diagnostics.sql_select             = NULL,
        @sys.diagnostics.table_name             = NULL;
    IF (v_this_thread_enabled = 'YES') THEN
        CALL sys.ps_setup_enable_thread(CONNECTION_ID());
    END IF;
    IF (@log_bin = 1) THEN
        SET sql_log_bin = @log_bin;
    END IF;
END
execute_prepared_stmt
BEGIN
    IF (@sys.debug IS NULL) THEN
        SET @sys.debug = sys.sys_get_config('debug', 'OFF');
    END IF;
    IF (in_query IS NULL OR LENGTH(in_query) < 4) THEN
       SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = "The @sys.execute_prepared_stmt.sql must contain a query";
    END IF;
    SET @sys.execute_prepared_stmt.sql = in_query;
    IF (@sys.debug = 'ON') THEN
        SELECT @sys.execute_prepared_stmt.sql AS 'Debug';
    END IF;
    PREPARE sys_execute_prepared_stmt FROM @sys.execute_prepared_stmt.sql;
    EXECUTE sys_execute_prepared_stmt;
    DEALLOCATE PREPARE sys_execute_prepared_stmt;
    SET @sys.execute_prepared_stmt.sql = NULL;
END
extract_schema_from_file_name
BEGIN
    RETURN LEFT(SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(path, '\\', '/'), '/', -2), '/', 1), 64);
END
extract_table_from_file_name
BEGIN
    RETURN LEFT(SUBSTRING_INDEX(REPLACE(SUBSTRING_INDEX(REPLACE(path, '\\', '/'), '/', -1), '@0024', '$'), '.', 1), 64);
END
format_bytes
BEGIN
  IF bytes IS NULL THEN RETURN NULL;
  ELSEIF bytes >= 1125899906842624 THEN RETURN CONCAT(ROUND(bytes / 1125899906842624, 2), ' PiB');
  ELSEIF bytes >= 1099511627776 THEN RETURN CONCAT(ROUND(bytes / 1099511627776, 2), ' TiB');
  ELSEIF bytes >= 1073741824 THEN RETURN CONCAT(ROUND(bytes / 1073741824, 2), ' GiB');
  ELSEIF bytes >= 1048576 THEN RETURN CONCAT(ROUND(bytes / 1048576, 2), ' MiB');
  ELSEIF bytes >= 1024 THEN RETURN CONCAT(ROUND(bytes / 1024, 2), ' KiB');
  ELSE RETURN CONCAT(ROUND(bytes, 0), ' bytes');
  END IF;
END
format_statement
BEGIN
  IF @sys.statement_truncate_len IS NULL THEN
      SET @sys.statement_truncate_len = sys_get_config('statement_truncate_len', 64);
  END IF;
  IF CHAR_LENGTH(statement) > @sys.statement_truncate_len THEN
      RETURN REPLACE(CONCAT(LEFT(statement, (@sys.statement_truncate_len/2)-2), ' ... ', RIGHT(statement, (@sys.statement_truncate_len/2)-2)), '\
', ' ');
  ELSE
      RETURN REPLACE(statement, '\
', ' ');
  END IF;
END
list_add
BEGIN
    IF (in_add_value IS NULL) THEN
        SIGNAL SQLSTATE '02200'
           SET MESSAGE_TEXT = 'Function sys.list_add: in_add_value input variable should not be NULL',
               MYSQL_ERRNO = 1138;
    END IF;
    IF (in_list IS NULL OR LENGTH(in_list) = 0) THEN
        RETURN in_add_value;
    END IF;
    RETURN (SELECT CONCAT(TRIM(BOTH ',' FROM TRIM(in_list)), ',', in_add_value));
END
format_path
BEGIN
  DECLARE v_dir VARCHAR(1024);
  DECLARE v_path VARCHAR(512);
  DECLARE path_separator CHAR(1) DEFAULT '/';
  IF @@global.version_compile_os LIKE 'win%' THEN
    SET path_separator = '\\';
  END IF;
  IF in_path LIKE '/private/%' THEN
    SET v_path = REPLACE(in_path, '/private', '');
  ELSE
    SET v_path = in_path;
  END IF;
  SET v_dir= IFNULL((SELECT VARIABLE_VALUE FROM information_schema.global_variables WHERE VARIABLE_NAME = 'innodb_data_home_dir'), '');
  IF v_path IS NULL THEN
    RETURN NULL;
  END IF;
  IF v_path LIKE CONCAT(@@global.datadir, IF(SUBSTRING(@@global.datadir, -1) = path_separator, '%', CONCAT(path_separator, '%'))) ESCAPE '|' THEN
    SET v_path = REPLACE(v_path, @@global.datadir, CONCAT('@@datadir', IF(SUBSTRING(@@global.datadir, -1) = path_separator, path_separator, '')));
    RETURN v_path;
  END IF;
  IF v_path LIKE CONCAT(@@global.tmpdir, IF(SUBSTRING(@@global.tmpdir, -1) = path_separator, '%', CONCAT(path_separator, '%'))) ESCAPE '|' THEN
    SET v_path = REPLACE(v_path, @@global.tmpdir, CONCAT('@@tmpdir', IF(SUBSTRING(@@global.tmpdir, -1) = path_separator, path_separator, '')));
    RETURN v_path;
  END IF;
  SET v_dir= IFNULL((SELECT VARIABLE_VALUE FROM information_schema.global_variables WHERE VARIABLE_NAME = 'innodb_data_home_dir'), '');
  IF v_path LIKE CONCAT(v_dir, IF(SUBSTRING(v_dir, -1) = path_separator, '%', CONCAT(path_separator, '%'))) ESCAPE '|' THEN
    SET v_path = REPLACE(v_path, v_dir, CONCAT('@@innodb_data_home_dir', IF(SUBSTRING(v_dir, -1) = path_separator, path_separator, '')));
    RETURN v_path;
  END IF;
  SET v_dir= IFNULL((SELECT VARIABLE_VALUE FROM information_schema.global_variables WHERE VARIABLE_NAME = 'innodb_log_group_home_dir'), '');
  IF v_path LIKE CONCAT(v_dir, IF(SUBSTRING(v_dir, -1) = path_separator, '%', CONCAT(path_separator, '%'))) ESCAPE '|' THEN
    SET v_path = REPLACE(v_path, v_dir, CONCAT('@@innodb_log_group_home_dir', IF(SUBSTRING(v_dir, -1) = path_separator, path_separator, '')));
    RETURN v_path;
  END IF;
  SET v_dir= IFNULL((SELECT VARIABLE_VALUE FROM information_schema.global_variables WHERE VARIABLE_NAME = 'slave_load_tmpdir'), '');
  IF v_path LIKE CONCAT(v_dir, IF(SUBSTRING(v_dir, -1) = path_separator, '%', CONCAT(path_separator, '%'))) ESCAPE '|' THEN
    SET v_path = REPLACE(v_path, v_dir, CONCAT('@@slave_load_tmpdir', IF(SUBSTRING(v_dir, -1) = path_separator, path_separator, '')));
    RETURN v_path;
  END IF;
  SET v_dir = IFNULL((SELECT VARIABLE_VALUE FROM information_schema.global_variables WHERE VARIABLE_NAME = 'innodb_undo_directory'), '');
  IF v_path LIKE CONCAT(v_dir, IF(SUBSTRING(v_dir, -1) = path_separator, '%', CONCAT(path_separator, '%'))) ESCAPE '|' THEN
    SET v_path = REPLACE(v_path, v_dir, CONCAT('@@innodb_undo_directory', IF(SUBSTRING(v_dir, -1) = path_separator, path_separator, '')));
    RETURN v_path;
  END IF;
  IF v_path LIKE CONCAT(@@global.basedir, IF(SUBSTRING(@@global.basedir, -1) = path_separator, '%', CONCAT(path_separator, '%'))) ESCAPE '|' THEN
    SET v_path = REPLACE(v_path, @@global.basedir, CONCAT('@@basedir', IF(SUBSTRING(@@global.basedir, -1) = path_separator, path_separator, '')));
    RETURN v_path;
  END IF;
  RETURN v_path;
END
format_time
BEGIN
  IF picoseconds IS NULL THEN RETURN NULL;
  ELSEIF picoseconds >= 604800000000000000 THEN RETURN CONCAT(ROUND(picoseconds / 604800000000000000, 2), ' w');
  ELSEIF picoseconds >= 86400000000000000 THEN RETURN CONCAT(ROUND(picoseconds / 86400000000000000, 2), ' d');
  ELSEIF picoseconds >= 3600000000000000 THEN RETURN CONCAT(ROUND(picoseconds / 3600000000000000, 2), ' h');
  ELSEIF picoseconds >= 60000000000000 THEN RETURN CONCAT(ROUND(picoseconds / 60000000000000, 2), ' m');
  ELSEIF picoseconds >= 1000000000000 THEN RETURN CONCAT(ROUND(picoseconds / 1000000000000, 2), ' s');
  ELSEIF picoseconds >= 1000000000 THEN RETURN CONCAT(ROUND(picoseconds / 1000000000, 2), ' ms');
  ELSEIF picoseconds >= 1000000 THEN RETURN CONCAT(ROUND(picoseconds / 1000000, 2), ' us');
  ELSEIF picoseconds >= 1000 THEN RETURN CONCAT(ROUND(picoseconds / 1000, 2), ' ns');
  ELSE RETURN CONCAT(picoseconds, ' ps');
  END IF;
END
list_drop
BEGIN
    IF (in_drop_value IS NULL) THEN
        SIGNAL SQLSTATE '02200'
           SET MESSAGE_TEXT = 'Function sys.list_drop: in_drop_value input variable should not be NULL',
               MYSQL_ERRNO = 1138;
    END IF;
    IF (in_list IS NULL OR LENGTH(in_list) = 0) THEN
        RETURN in_list;
    END IF;
    RETURN (SELECT TRIM(BOTH ',' FROM REPLACE(REPLACE(CONCAT(',', in_list), CONCAT(',', in_drop_value), ''), CONCAT(', ', in_drop_value), '')));
END
optimizer_switch_choice
BEGIN
  DECLARE tmp VARCHAR(1024);
  DECLARE opt VARCHAR(1024);
  DECLARE start INT;
  DECLARE end INT;
  DECLARE pos INT;
  set tmp=concat(@@optimizer_switch,",");
  CREATE OR REPLACE TEMPORARY TABLE tmp_opt_switch (a varchar(64), opt CHAR(3)) character set latin1 engine=heap;
  set start=1;
  FIND_OPTIONS:
  LOOP
    set pos= INSTR(SUBSTR(tmp, start), ",");
    if (pos = 0) THEN
       LEAVE FIND_OPTIONS;
    END IF;
    set opt= MID(tmp, start, pos-1);
    set end= INSTR(opt, "=");
    insert into tmp_opt_switch values(LEFT(opt,end-1),SUBSTR(opt,end+1));
    set start=start + pos;
  END LOOP;
  SELECT  t.a as "option",t.opt from tmp_opt_switch as t where t.opt = on_off order by a;
  DROP TEMPORARY TABLE tmp_opt_switch;
END
optimizer_switch_off
BEGIN
  call optimizer_switch_choice("off");
END
optimizer_switch_on
BEGIN
  call optimizer_switch_choice("on");
END
ps_is_account_enabled
BEGIN
    RETURN IF(EXISTS(SELECT 1
                       FROM performance_schema.setup_actors
                      WHERE (`HOST` = '%' OR in_host LIKE `HOST`)
                        AND (`USER` = '%' OR `USER` = in_user)
                    ),
              'YES', 'NO'
           );
END
ps_is_instrument_default_enabled
BEGIN
    DECLARE v_enabled ENUM('YES', 'NO');
    SET v_enabled = IF(in_instrument LIKE 'wait/io/file/%'
                        OR in_instrument LIKE 'wait/io/table/%'
                        OR in_instrument LIKE 'statement/%'
                        OR in_instrument LIKE 'memory/performance_schema/%'
                        OR in_instrument IN ('wait/lock/table/sql/handler', 'idle')
               
                      ,
                       'YES',
                       'NO'
                    );
    RETURN v_enabled;
END
ps_is_instrument_default_timed
BEGIN
    DECLARE v_timed ENUM('YES', 'NO');
    SET v_timed = IF(in_instrument LIKE 'wait/io/file/%'
                        OR in_instrument LIKE 'wait/io/table/%'
                        OR in_instrument LIKE 'statement/%'
                        OR in_instrument IN ('wait/lock/table/sql/handler', 'idle')
               
                      ,
                       'YES',
                       'NO'
                    );
    RETURN v_timed;
END
ps_is_thread_instrumented
BEGIN
    DECLARE v_enabled ENUM('YES', 'NO', 'UNKNOWN');
    IF (in_connection_id IS NULL) THEN
        RETURN NULL;
    END IF;
    SELECT INSTRUMENTED INTO v_enabled
      FROM performance_schema.threads
     WHERE PROCESSLIST_ID = in_connection_id;
    IF (v_enabled IS NULL) THEN
        RETURN 'UNKNOWN';
    ELSE
        RETURN v_enabled;
    END IF;
END
ps_is_consumer_enabled
BEGIN
    RETURN (
        SELECT (CASE
                   WHEN c.NAME = 'global_instrumentation' THEN c.ENABLED
                   WHEN c.NAME = 'thread_instrumentation' THEN IF(cg.ENABLED = 'YES' AND c.ENABLED = 'YES', 'YES', 'NO')
                   WHEN c.NAME LIKE '%\\_digest'           THEN IF(cg.ENABLED = 'YES' AND c.ENABLED = 'YES', 'YES', 'NO')
                   WHEN c.NAME LIKE '%\\_current'          THEN IF(cg.ENABLED = 'YES' AND ct.ENABLED = 'YES' AND c.ENABLED = 'YES', 'YES', 'NO')
                   ELSE IF(cg.ENABLED = 'YES' AND ct.ENABLED = 'YES' AND c.ENABLED = 'YES'
                           AND ( SELECT cc.ENABLED FROM performance_schema.setup_consumers cc WHERE NAME = CONCAT(SUBSTRING_INDEX(c.NAME, '_', 2), '_current')
                               ) = 'YES', 'YES', 'NO')
                END) AS IsEnabled
          FROM performance_schema.setup_consumers c
               INNER JOIN performance_schema.setup_consumers cg
               INNER JOIN performance_schema.setup_consumers ct
         WHERE cg.NAME       = 'global_instrumentation'
               AND ct.NAME   = 'thread_instrumentation'
               AND c.NAME    = in_consumer
       );
END
ps_setup_disable_background_threads
BEGIN
    UPDATE performance_schema.threads
       SET instrumented = 'NO'
     WHERE type = 'BACKGROUND';
    SELECT CONCAT('Disabled ', @rows := ROW_COUNT(), ' background thread', IF(@rows != 1, 's', '')) AS summary;
END
ps_setup_disable_consumer
BEGIN
    UPDATE performance_schema.setup_consumers
       SET enabled = 'NO'
     WHERE name LIKE CONCAT('%', consumer, '%');
    SELECT CONCAT('Disabled ', @rows := ROW_COUNT(), ' consumer', IF(@rows != 1, 's', '')) AS summary;
END
ps_setup_disable_instrument
BEGIN
    UPDATE performance_schema.setup_instruments
       SET enabled = 'NO', timed = 'NO'
     WHERE name LIKE CONCAT('%', in_pattern, '%');
    SELECT CONCAT('Disabled ', @rows := ROW_COUNT(), ' instrument', IF(@rows != 1, 's', '')) AS summary;
END
ps_setup_disable_thread
BEGIN
    UPDATE performance_schema.threads
       SET instrumented = 'NO'
     WHERE processlist_id = in_connection_id;
    SELECT CONCAT('Disabled ', @rows := ROW_COUNT(), ' thread', IF(@rows != 1, 's', '')) AS summary;
END
ps_setup_enable_background_threads
BEGIN
    UPDATE performance_schema.threads
       SET instrumented = 'YES'
     WHERE type = 'BACKGROUND';
    SELECT CONCAT('Enabled ', @rows := ROW_COUNT(), ' background thread', IF(@rows != 1, 's', '')) AS summary;
END
ps_setup_enable_consumer
BEGIN
    UPDATE performance_schema.setup_consumers
       SET enabled = 'YES'
     WHERE name LIKE CONCAT('%', consumer, '%');
    SELECT CONCAT('Enabled ', @rows := ROW_COUNT(), ' consumer', IF(@rows != 1, 's', '')) AS summary;
END
ps_setup_enable_instrument
BEGIN
    UPDATE performance_schema.setup_instruments
       SET enabled = 'YES', timed = 'YES'
     WHERE name LIKE CONCAT('%', in_pattern, '%');
    SELECT CONCAT('Enabled ', @rows := ROW_COUNT(), ' instrument', IF(@rows != 1, 's', '')) AS summary;
END
ps_setup_enable_thread
BEGIN
    UPDATE performance_schema.threads
       SET instrumented = 'YES'
     WHERE processlist_id = in_connection_id;
    SELECT CONCAT('Enabled ', @rows := ROW_COUNT(), ' thread', IF(@rows != 1, 's', '')) AS summary;
END
ps_setup_reload_saved
BEGIN
    DECLARE v_done bool DEFAULT FALSE;
    DECLARE v_lock_result INT;
    DECLARE v_lock_used_by BIGINT;
    DECLARE v_signal_message TEXT;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SIGNAL SQLSTATE VALUE '90001'
           SET MESSAGE_TEXT = 'An error occurred, was sys.ps_setup_save() run before this procedure?';
    END;
    SET @log_bin := @@sql_log_bin;
    SET sql_log_bin = 0;
    SELECT IS_USED_LOCK('sys.ps_setup_save') INTO v_lock_used_by;
    IF (v_lock_used_by != CONNECTION_ID()) THEN
        SET v_signal_message = CONCAT('The sys.ps_setup_save lock is currently owned by ', v_lock_used_by);
        SIGNAL SQLSTATE VALUE '90002'
           SET MESSAGE_TEXT = v_signal_message;
    END IF;
    DELETE FROM performance_schema.setup_actors;
    INSERT INTO performance_schema.setup_actors SELECT * FROM tmp_setup_actors;
    BEGIN
        DECLARE v_name varchar(64);
        DECLARE v_enabled enum('YES', 'NO');
        DECLARE c_consumers CURSOR FOR SELECT NAME, ENABLED FROM tmp_setup_consumers;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;
        SET v_done = FALSE;
        OPEN c_consumers;
        c_consumers_loop: LOOP
            FETCH c_consumers INTO v_name, v_enabled;
            IF v_done THEN
               LEAVE c_consumers_loop;
            END IF;
            UPDATE performance_schema.setup_consumers
               SET ENABLED = v_enabled
             WHERE NAME = v_name;
         END LOOP;
         CLOSE c_consumers;
    END;
    UPDATE performance_schema.setup_instruments
     INNER JOIN tmp_setup_instruments USING (NAME)
       SET performance_schema.setup_instruments.ENABLED = tmp_setup_instruments.ENABLED,
           performance_schema.setup_instruments.TIMED   = tmp_setup_instruments.TIMED;
    BEGIN
        DECLARE v_thread_id bigint unsigned;
        DECLARE v_instrumented enum('YES', 'NO');
        DECLARE c_threads CURSOR FOR SELECT THREAD_ID, INSTRUMENTED FROM tmp_threads;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;
        SET v_done = FALSE;
        OPEN c_threads;
        c_threads_loop: LOOP
            FETCH c_threads INTO v_thread_id, v_instrumented;
            IF v_done THEN
               LEAVE c_threads_loop;
            END IF;
            UPDATE performance_schema.threads
               SET INSTRUMENTED = v_instrumented
             WHERE THREAD_ID = v_thread_id;
        END LOOP;
        CLOSE c_threads;
    END;
    UPDATE performance_schema.threads
       SET INSTRUMENTED = IF(PROCESSLIST_USER IS NOT NULL,
                             sys.ps_is_account_enabled(PROCESSLIST_HOST, PROCESSLIST_USER),
                             'YES')
     WHERE THREAD_ID NOT IN (SELECT THREAD_ID FROM tmp_threads);
    DROP TEMPORARY TABLE tmp_setup_actors;
    DROP TEMPORARY TABLE tmp_setup_consumers;
    DROP TEMPORARY TABLE tmp_setup_instruments;
    DROP TEMPORARY TABLE tmp_threads;
    SELECT RELEASE_LOCK('sys.ps_setup_save') INTO v_lock_result;
    SET sql_log_bin = @log_bin;
END
ps_setup_show_disabled_consumers
BEGIN
    SELECT name AS disabled_consumers
      FROM performance_schema.setup_consumers
     WHERE enabled = 'NO'
     ORDER BY disabled_consumers;
END
ps_setup_show_disabled_instruments
BEGIN
    SELECT name AS disabled_instruments, timed
      FROM performance_schema.setup_instruments
     WHERE enabled = 'NO'
     ORDER BY disabled_instruments;
END
ps_setup_show_enabled_consumers
BEGIN
    SELECT name AS enabled_consumers
      FROM performance_schema.setup_consumers
     WHERE enabled = 'YES'
     ORDER BY enabled_consumers;
END
ps_setup_show_enabled_instruments
BEGIN
    SELECT name AS enabled_instruments, timed
      FROM performance_schema.setup_instruments
     WHERE enabled = 'YES'
     ORDER BY enabled_instruments;
END
ps_statement_avg_latency_histogram
BEGIN
SELECT CONCAT('\
',
       '\
  . = 1 unit',
       '\
  * = 2 units',
       '\
  # = 3 units\
',
       @label := CONCAT(@label_inner := CONCAT('\
(0 - ',
                                               ROUND((@bucket_size := (SELECT ROUND((MAX(avg_us) - MIN(avg_us)) / (@buckets := 16)) AS size
                                                                         FROM sys.x$ps_digest_avg_latency_distribution)) / (@unit_div := 1000)),
                                                (@unit := 'ms'), ')'),
                        REPEAT(' ', (@max_label_size := ((1 + LENGTH(ROUND((@bucket_size * 15) / @unit_div)) + 3 + LENGTH(ROUND(@bucket_size * 16) / @unit_div)) + 1)) - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us <= @bucket_size), 0)),
       REPEAT(' ', (@max_label_len := (@max_label_size + LENGTH((@total_queries := (SELECT SUM(cnt) FROM sys.x$ps_digest_avg_latency_distribution)))) + 1) - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < (@one_unit := 40), '.', IF(@count_in_bucket < (@two_unit := 80), '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND(@bucket_size / @unit_div), ' - ', ROUND((@bucket_size * 2) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size AND b1.avg_us <= @bucket_size * 2), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 2) / @unit_div), ' - ', ROUND((@bucket_size * 3) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 2 AND b1.avg_us <= @bucket_size * 3), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 3) / @unit_div), ' - ', ROUND((@bucket_size * 4) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 3 AND b1.avg_us <= @bucket_size * 4), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 4) / @unit_div), ' - ', ROUND((@bucket_size * 5) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 4 AND b1.avg_us <= @bucket_size * 5), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 5) / @unit_div), ' - ', ROUND((@bucket_size * 6) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 5 AND b1.avg_us <= @bucket_size * 6), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 6) / @unit_div), ' - ', ROUND((@bucket_size * 7) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 6 AND b1.avg_us <= @bucket_size * 7), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 7) / @unit_div), ' - ', ROUND((@bucket_size * 8) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 7 AND b1.avg_us <= @bucket_size * 8), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 8) / @unit_div), ' - ', ROUND((@bucket_size * 9) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 8 AND b1.avg_us <= @bucket_size * 9), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 9) / @unit_div), ' - ', ROUND((@bucket_size * 10) / @unit_div), @unit, ')'),
                         REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                         @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                       FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                      WHERE b1.avg_us > @bucket_size * 9 AND b1.avg_us <= @bucket_size * 10), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 10) / @unit_div), ' - ', ROUND((@bucket_size * 11) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 10 AND b1.avg_us <= @bucket_size * 11), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 11) / @unit_div), ' - ', ROUND((@bucket_size * 12) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 11 AND b1.avg_us <= @bucket_size * 12), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 12) / @unit_div), ' - ', ROUND((@bucket_size * 13) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 12 AND b1.avg_us <= @bucket_size * 13), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 13) / @unit_div), ' - ', ROUND((@bucket_size * 14) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 13 AND b1.avg_us <= @bucket_size * 14), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 14) / @unit_div), ' - ', ROUND((@bucket_size * 15) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 14 AND b1.avg_us <= @bucket_size * 15), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       @label := CONCAT(@label_inner := CONCAT('\
(', ROUND((@bucket_size * 15) / @unit_div), ' - ', ROUND((@bucket_size * 16) / @unit_div), @unit, ')'),
                        REPEAT(' ', @max_label_size - LENGTH(@label_inner)),
                        @count_in_bucket := IFNULL((SELECT SUM(cnt)
                                                      FROM sys.x$ps_digest_avg_latency_distribution AS b1
                                                     WHERE b1.avg_us > @bucket_size * 15 AND b1.avg_us <= @bucket_size * 16), 0)),
       REPEAT(' ', @max_label_len - LENGTH(@label)), '| ',
       IFNULL(REPEAT(IF(@count_in_bucket < @one_unit, '.', IF(@count_in_bucket < @two_unit, '*', '#')),
       \t             IF(@count_in_bucket < @one_unit, @count_in_bucket,
       \t             \tIF(@count_in_bucket < @two_unit, @count_in_bucket / 2, @count_in_bucket / 3))), ''),
       '\
\
  Total Statements: ', @total_queries, '; Buckets: ', @buckets , '; Bucket Size: ', ROUND(@bucket_size / @unit_div) , ' ', @unit, ';\
'
      ) AS `Performance Schema Statement Digest Average Latency Histogram`;
END
ps_thread_account
BEGIN
    RETURN (SELECT IF(
                      type = 'FOREGROUND',
                      CONCAT(processlist_user, '@', processlist_host),
                      type
                     ) AS account
              FROM `performance_schema`.`threads`
             WHERE thread_id = in_thread_id);
END
ps_setup_reset_to_default
BEGIN
    SET @query = 'DELETE\
                    FROM performance_schema.setup_actors\
                   WHERE NOT (HOST = ''%'' AND USER = ''%'' AND ROLE = ''%'')';
    IF (in_verbose) THEN
        SELECT CONCAT('Resetting: setup_actors\
', REPLACE(@query, '  ', '')) AS status;
    END IF;
    PREPARE reset_stmt FROM @query;
    EXECUTE reset_stmt;
    DEALLOCATE PREPARE reset_stmt;
    SET @query = 'INSERT IGNORE INTO performance_schema.setup_actors\
                  VALUES (''%'', ''%'', ''%'', ''YES'', ''YES'')';
    IF (in_verbose) THEN
        SELECT CONCAT('Resetting: setup_actors\
', REPLACE(@query, '  ', '')) AS status;
    END IF;
    PREPARE reset_stmt FROM @query;
    EXECUTE reset_stmt;
    DEALLOCATE PREPARE reset_stmt;
    SET @query = 'UPDATE performance_schema.setup_instruments\
                     SET ENABLED = sys.ps_is_instrument_default_enabled(NAME),\
                         TIMED   = sys.ps_is_instrument_default_timed(NAME)';
    IF (in_verbose) THEN
        SELECT CONCAT('Resetting: setup_instruments\
', REPLACE(@query, '  ', '')) AS status;
    END IF;
    PREPARE reset_stmt FROM @query;
    EXECUTE reset_stmt;
    DEALLOCATE PREPARE reset_stmt;
    SET @query = 'UPDATE performance_schema.setup_consumers\
                     SET ENABLED = IF(NAME IN (''events_statements_current'', ''events_transactions_current'', ''global_instrumentation'', ''thread_instrumentation'', ''statements_digest''), ''YES'', ''NO'')';
    IF (in_verbose) THEN
        SELECT CONCAT('Resetting: setup_consumers\
', REPLACE(@query, '  ', '')) AS status;
    END IF;
    PREPARE reset_stmt FROM @query;
    EXECUTE reset_stmt;
    DEALLOCATE PREPARE reset_stmt;
    SET @query = 'DELETE\
                    FROM performance_schema.setup_objects\
                   WHERE NOT (OBJECT_TYPE IN (''EVENT'', ''FUNCTION'', ''PROCEDURE'', ''TABLE'', ''TRIGGER'') AND OBJECT_NAME = ''%''\
                     AND (OBJECT_SCHEMA = ''mysql''              AND ENABLED = ''NO''  AND TIMED = ''NO'' )\
                      OR (OBJECT_SCHEMA = ''performance_schema'' AND ENABLED = ''NO''  AND TIMED = ''NO'' )\
                      OR (OBJECT_SCHEMA = ''information_schema'' AND ENABLED = ''NO''  AND TIMED = ''NO'' )\
                      OR (OBJECT_SCHEMA = ''%''                  AND ENABLED = ''YES'' AND TIMED = ''YES''))';
    IF (in_verbose) THEN
        SELECT CONCAT('Resetting: setup_objects\
', REPLACE(@query, '  ', '')) AS status;
    END IF;
    PREPARE reset_stmt FROM @query;
    EXECUTE reset_stmt;
    DEALLOCATE PREPARE reset_stmt;
    SET @query = 'INSERT IGNORE INTO performance_schema.setup_objects\
                  VALUES (''EVENT''    , ''mysql''             , ''%'', ''NO'' , ''NO'' ),\
                         (''EVENT''    , ''performance_schema'', ''%'', ''NO'' , ''NO'' ),\
                         (''EVENT''    , ''information_schema'', ''%'', ''NO'' , ''NO'' ),\
                         (''EVENT''    , ''%''                 , ''%'', ''YES'', ''YES''),\
                         (''FUNCTION'' , ''mysql''             , ''%'', ''NO'' , ''NO'' ),\
                         (''FUNCTION'' , ''performance_schema'', ''%'', ''NO'' , ''NO'' ),\
                         (''FUNCTION'' , ''information_schema'', ''%'', ''NO'' , ''NO'' ),\
                         (''FUNCTION'' , ''%''                 , ''%'', ''YES'', ''YES''),\
                         (''PROCEDURE'', ''mysql''             , ''%'', ''NO'' , ''NO'' ),\
                         (''PROCEDURE'', ''performance_schema'', ''%'', ''NO'' , ''NO'' ),\
                         (''PROCEDURE'', ''information_schema'', ''%'', ''NO'' , ''NO'' ),\
                         (''PROCEDURE'', ''%''                 , ''%'', ''YES'', ''YES''),\
                         (''TABLE''    , ''mysql''             , ''%'', ''NO'' , ''NO'' ),\
                         (''TABLE''    , ''performance_schema'', ''%'', ''NO'' , ''NO'' ),\
                         (''TABLE''    , ''information_schema'', ''%'', ''NO'' , ''NO'' ),\
                         (''TABLE''    , ''%''                 , ''%'', ''YES'', ''YES''),\
                         (''TRIGGER''  , ''mysql''             , ''%'', ''NO'' , ''NO'' ),\
                         (''TRIGGER''  , ''performance_schema'', ''%'', ''NO'' , ''NO'' ),\
                         (''TRIGGER''  , ''information_schema'', ''%'', ''NO'' , ''NO'' ),\
                         (''TRIGGER''  , ''%''                 , ''%'', ''YES'', ''YES'')';
    IF (in_verbose) THEN
        SELECT CONCAT('Resetting: setup_objects\
', REPLACE(@query, '  ', '')) AS status;
    END IF;
    PREPARE reset_stmt FROM @query;
    EXECUTE reset_stmt;
    DEALLOCATE PREPARE reset_stmt;
    SET @query = 'UPDATE performance_schema.threads\
                     SET INSTRUMENTED = ''YES''';
    IF (in_verbose) THEN
        SELECT CONCAT('Resetting: threads\
', REPLACE(@query, '  ', '')) AS status;
    END IF;
    PREPARE reset_stmt FROM @query;
    EXECUTE reset_stmt;
    DEALLOCATE PREPARE reset_stmt;
END
ps_thread_id
BEGIN
    RETURN (SELECT THREAD_ID
              FROM `performance_schema`.`threads`
             WHERE PROCESSLIST_ID = IFNULL(in_connection_id, CONNECTION_ID())
           );
END
ps_trace_thread
BEGIN
    DECLARE v_done bool DEFAULT FALSE;
    DECLARE v_start, v_runtime DECIMAL(20,2) DEFAULT 0.0;
    DECLARE v_min_event_id bigint unsigned DEFAULT 0;
    DECLARE v_this_thread_enabed ENUM('YES', 'NO');
    DECLARE v_event longtext;
    DECLARE c_stack CURSOR FOR
        SELECT CONCAT(IF(nesting_event_id IS NOT NULL, CONCAT(nesting_event_id, ' -> '), ''),
                    event_id, '; ', event_id, ' [label="',
                    '(', format_pico_time(timer_wait), ') ',
                    IF (event_name NOT LIKE 'wait/io%',
                        SUBSTRING_INDEX(event_name, '/', -2),
                        IF (event_name NOT LIKE 'wait/io/file%' OR event_name NOT LIKE 'wait/io/socket%',
                            SUBSTRING_INDEX(event_name, '/', -4),
                            event_name)
                        ),
                    IF (event_name LIKE 'statement/%', IFNULL(CONCAT('\
', wait_info), ''), ''),
                    IF (in_debug AND event_name LIKE 'wait%', wait_info, ''),
                    '", ',
                    CASE WHEN event_name LIKE 'wait/io/file%' THEN
                           'shape=box, style=filled, color=red'
                         WHEN event_name LIKE 'wait/io/table%' THEN
                           'shape=box, style=filled, color=green'
                         WHEN event_name LIKE 'wait/io/socket%' THEN
                           'shape=box, style=filled, color=yellow'
                         WHEN event_name LIKE 'wait/synch/mutex%' THEN
                           'style=filled, color=lightskyblue'
                         WHEN event_name LIKE 'wait/synch/cond%' THEN
                           'style=filled, color=darkseagreen3'
                         WHEN event_name LIKE 'wait/synch/rwlock%' THEN
                           'style=filled, color=orchid'
                         WHEN event_name LIKE 'wait/lock%' THEN
                           'shape=box, style=filled, color=tan'
                         WHEN event_name LIKE 'statement/%' THEN
                           CONCAT('shape=box, style=bold',
                                  CASE WHEN event_name LIKE 'statement/com/%' THEN
                                         ' style=filled, color=darkseagreen'
                                       ELSE
                                         IF((timer_wait/1000000000000) > @@log_slow_query_time,
                                            ' style=filled, color=red',
                                            ' style=filled, color=lightblue')
                                  END
                           )
                         WHEN event_name LIKE 'stage/%' THEN
                           'style=filled, color=slategray3'
                         WHEN event_name LIKE '%idle%' THEN
                           'shape=box, style=filled, color=firebrick3'
                         ELSE '' END,
                     '];\
'
                   ) event, event_id
        FROM (
             (SELECT thread_id, event_id, event_name, timer_wait, timer_start, nesting_event_id,
                     CONCAT(sql_text, '\
',
                            'errors: ', errors, '\
',
                            'warnings: ', warnings, '\
',
                            'lock time: ', format_pico_time(lock_time),'\
',
                            'rows affected: ', rows_affected, '\
',
                            'rows sent: ', rows_sent, '\
',
                            'rows examined: ', rows_examined, '\
',
                            'tmp tables: ', created_tmp_tables, '\
',
                            'tmp disk tables: ', created_tmp_disk_tables, '\
'
                            'select scan: ', select_scan, '\
',
                            'select full join: ', select_full_join, '\
',
                            'select full range join: ', select_full_range_join, '\
',
                            'select range: ', select_range, '\
',
                            'select range check: ', select_range_check, '\
',
                            'sort merge passes: ', sort_merge_passes, '\
',
                            'sort rows: ', sort_rows, '\
',
                            'sort range: ', sort_range, '\
',
                            'sort scan: ', sort_scan, '\
',
                            'no index used: ', IF(no_index_used, 'TRUE', 'FALSE'), '\
',
                            'no good index used: ', IF(no_good_index_used, 'TRUE', 'FALSE'), '\
'
                     ) AS wait_info
                FROM performance_schema.events_statements_history_long
               WHERE thread_id = in_thread_id AND event_id > v_min_event_id)
             UNION
             (SELECT thread_id, event_id, event_name, timer_wait, timer_start, nesting_event_id, null AS wait_info
                FROM performance_schema.events_stages_history_long
               WHERE thread_id = in_thread_id AND event_id > v_min_event_id)
             UNION
             (SELECT thread_id, event_id,
                     CONCAT(event_name,
                            IF(event_name NOT LIKE 'wait/synch/mutex%', IFNULL(CONCAT(' - ', operation), ''), ''),
                            IF(number_of_bytes IS NOT NULL, CONCAT(' ', number_of_bytes, ' bytes'), ''),
                            IF(event_name LIKE 'wait/io/file%', '\
', ''),
                            IF(object_schema IS NOT NULL, CONCAT('\
Object: ', object_schema, '.'), ''),
                            IF(object_name IS NOT NULL,
                               IF (event_name LIKE 'wait/io/socket%',
                                   CONCAT('\
', IF (object_name LIKE ':0%', @@socket, object_name)),
                                   object_name),
                               ''
                            ),
                            IF(index_name IS NOT NULL, CONCAT(' Index: ', index_name), ''), '\
'
                     ) AS event_name,
                     timer_wait, timer_start, nesting_event_id, source AS wait_info
                FROM performance_schema.events_waits_history_long
               WHERE thread_id = in_thread_id AND event_id > v_min_event_id)
           ) events
       ORDER BY event_id;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;
    SET @log_bin := @@sql_log_bin;
    SET sql_log_bin = 0;
    SELECT INSTRUMENTED INTO v_this_thread_enabed FROM performance_schema.threads WHERE PROCESSLIST_ID = CONNECTION_ID();
    CALL sys.ps_setup_disable_thread(CONNECTION_ID());
    IF (in_auto_setup) THEN
        CALL sys.ps_setup_save(0);
        DELETE FROM performance_schema.setup_actors;
        UPDATE performance_schema.threads
           SET INSTRUMENTED = IF(THREAD_ID = in_thread_id, 'YES', 'NO');
        UPDATE performance_schema.setup_consumers
           SET ENABLED = 'YES'
         WHERE NAME NOT LIKE '%\\_history';
        UPDATE performance_schema.setup_instruments
           SET ENABLED = 'YES',
               TIMED   = 'YES';
    END IF;
    IF (in_start_fresh) THEN
        TRUNCATE performance_schema.events_statements_history_long;
        TRUNCATE performance_schema.events_stages_history_long;
        TRUNCATE performance_schema.events_waits_history_long;
    END IF;
    DROP TEMPORARY TABLE IF EXISTS tmp_events;
    CREATE TEMPORARY TABLE tmp_events (
      event_id bigint unsigned NOT NULL,
      event longblob,
      PRIMARY KEY (event_id)
    );
    INSERT INTO tmp_events VALUES (0, CONCAT('digraph events { rankdir=LR; nodesep=0.10;\
',
                                             '// Stack created .....: ', NOW(), '\
',
                                             '// MySQL version .....: ', VERSION(), '\
',
                                             '// MySQL hostname ....: ', @@hostname, '\
',
                                             '// MySQL port ........: ', @@port, '\
',
                                             '// MySQL socket ......: ', @@socket, '\
',
                                             '// MySQL user ........: ', CURRENT_USER(), '\
'));
    SELECT CONCAT('Data collection starting for THREAD_ID = ', in_thread_id) AS 'Info';
    SET v_min_event_id = 0,
        v_start        = UNIX_TIMESTAMP(),
        in_interval    = IFNULL(in_interval, 1.00),
        in_max_runtime = IFNULL(in_max_runtime, 60.00);
    WHILE (v_runtime < in_max_runtime
           AND (SELECT INSTRUMENTED FROM performance_schema.threads WHERE THREAD_ID = in_thread_id) = 'YES') DO
        SET v_done = FALSE;
        OPEN c_stack;
        c_stack_loop: LOOP
            FETCH c_stack INTO v_event, v_min_event_id;
            IF v_done THEN
                LEAVE c_stack_loop;
            END IF;
            IF (LENGTH(v_event) > 0) THEN
                INSERT INTO tmp_events VALUES (v_min_event_id, v_event);
            END IF;
        END LOOP;
        CLOSE c_stack;
        SELECT SLEEP(in_interval) INTO @sleep;
        SET v_runtime = (UNIX_TIMESTAMP() - v_start);
    END WHILE;
    INSERT INTO tmp_events VALUES (v_min_event_id+1, '}');
    SET @query = CONCAT('SELECT event FROM tmp_events ORDER BY event_id INTO OUTFILE ''', in_outfile, ''' FIELDS ESCAPED BY '''' LINES TERMINATED BY ''''');
    PREPARE stmt_output FROM @query;
    EXECUTE stmt_output;
    DEALLOCATE PREPARE stmt_output;
    SELECT CONCAT('Stack trace written to ', in_outfile) AS 'Info';
    SELECT CONCAT('dot -Tpdf -o /tmp/stack_', in_thread_id, '.pdf ', in_outfile) AS 'Convert to PDF';
    SELECT CONCAT('dot -Tpng -o /tmp/stack_', in_thread_id, '.png ', in_outfile) AS 'Convert to PNG';
    DROP TEMPORARY TABLE tmp_events;
    IF (in_auto_setup) THEN
        CALL sys.ps_setup_reload_saved();
    END IF;
    IF (v_this_thread_enabed = 'YES') THEN
        CALL sys.ps_setup_enable_thread(CONNECTION_ID());
    END IF;
    SET sql_log_bin = @log_bin;
END
quote_identifier
BEGIN
    RETURN CONCAT('`', REPLACE(in_identifier, '`', '``'), '`');
END
statement_performance_analyzer
BEGIN
    DECLARE v_table_exists, v_tmp_digests_table_exists, v_custom_view_exists ENUM('', 'BASE TABLE', 'VIEW', 'TEMPORARY') DEFAULT '';
    DECLARE v_this_thread_enabled ENUM('YES', 'NO');
    DECLARE v_force_new_snapshot BOOLEAN DEFAULT FALSE;
    DECLARE v_digests_table VARCHAR(133);
    DECLARE v_quoted_table, v_quoted_custom_view VARCHAR(133) DEFAULT '';
    DECLARE v_table_db, v_table_name, v_custom_db, v_custom_name VARCHAR(64);
    DECLARE v_digest_table_template, v_checksum_ref, v_checksum_table text;
    DECLARE v_sql longtext;
    DECLARE v_error_msg VARCHAR(128);
    SELECT INSTRUMENTED INTO v_this_thread_enabled FROM performance_schema.threads WHERE PROCESSLIST_ID = CONNECTION_ID();
    IF (v_this_thread_enabled = 'YES') THEN
        CALL sys.ps_setup_disable_thread(CONNECTION_ID());
    END IF;
    SET @log_bin := @@sql_log_bin;
    IF (@log_bin = 1) THEN
        SET sql_log_bin = 0;
    END IF;
    IF (@sys.statement_performance_analyzer.limit IS NULL) THEN
        SET @sys.statement_performance_analyzer.limit = sys.sys_get_config('statement_performance_analyzer.limit', '100');
    END IF;
    IF (@sys.debug IS NULL) THEN
        SET @sys.debug                                = sys.sys_get_config('debug'                               , 'OFF');
    END IF;
    IF (in_table = 'NOW()') THEN
        SET v_force_new_snapshot = TRUE,
            in_table             = NULL;
    ELSEIF (in_table IS NOT NULL) THEN
        IF (NOT INSTR(in_table, '.')) THEN
            SET v_table_db   = DATABASE(),
                v_table_name = in_table;
        ELSE
            SET v_table_db   = SUBSTRING_INDEX(in_table, '.', 1);
            SET v_table_name = SUBSTRING(in_table, CHAR_LENGTH(v_table_db)+2);
        END IF;
        SET v_quoted_table = CONCAT('`', v_table_db, '`.`', v_table_name, '`');
        IF (@sys.debug = 'ON') THEN
            SELECT CONCAT('in_table is: db = ''', v_table_db, ''', table = ''', v_table_name, '''') AS 'Debug';
        END IF;
        IF (v_table_db = DATABASE() AND (v_table_name = 'tmp_digests' OR v_table_name = 'tmp_digests_delta')) THEN
            SET v_error_msg = CONCAT('Invalid value for in_table: ', v_quoted_table, ' is reserved table name.');
            SIGNAL SQLSTATE '45000'
               SET MESSAGE_TEXT = v_error_msg;
        END IF;
        CALL sys.table_exists(v_table_db, v_table_name, v_table_exists);
        IF (@sys.debug = 'ON') THEN
            SELECT CONCAT('v_table_exists = ', v_table_exists) AS 'Debug';
        END IF;
        IF (v_table_exists = 'BASE TABLE') THEN
            SET v_checksum_ref = (
                 SELECT GROUP_CONCAT(CONCAT(COLUMN_NAME, COLUMN_TYPE) ORDER BY ORDINAL_POSITION) AS Checksum
                   FROM information_schema.COLUMNS
                  WHERE TABLE_SCHEMA = 'performance_schema' AND TABLE_NAME = 'events_statements_summary_by_digest'
                ),
                v_checksum_table = (
                 SELECT GROUP_CONCAT(CONCAT(COLUMN_NAME, COLUMN_TYPE) ORDER BY ORDINAL_POSITION) AS Checksum
                   FROM information_schema.COLUMNS
                  WHERE TABLE_SCHEMA = v_table_db AND TABLE_NAME = v_table_name
                );
            IF (v_checksum_ref <> v_checksum_table) THEN
                SET v_error_msg = CONCAT('The table ',
                                         IF(CHAR_LENGTH(v_quoted_table) > 93, CONCAT('...', SUBSTRING(v_quoted_table, -90)), v_quoted_table),
                                         ' has the wrong definition.');
                SIGNAL SQLSTATE '45000'
                   SET MESSAGE_TEXT = v_error_msg;
            END IF;
        END IF;
    END IF;
    IF (in_views IS NULL OR in_views = '') THEN
        SET in_views = 'with_runtimes_in_95th_percentile,analysis,with_errors_or_warnings,with_full_table_scans,with_sorting,with_temp_tables';
    END IF;
    CALL sys.table_exists(DATABASE(), 'tmp_digests', v_tmp_digests_table_exists);
    IF (@sys.debug = 'ON') THEN
        SELECT CONCAT('v_tmp_digests_table_exists = ', v_tmp_digests_table_exists) AS 'Debug';
    END IF;
    CASE
        WHEN in_action IN ('snapshot', 'overall') THEN
            IF (in_table IS NOT NULL) THEN
                IF (NOT v_table_exists IN ('TEMPORARY', 'BASE TABLE')) THEN
                    SET v_error_msg = CONCAT('The ', in_action, ' action requires in_table to be NULL, NOW() or specify an existing table.',
                                             ' The table ',
                                             IF(CHAR_LENGTH(v_quoted_table) > 16, CONCAT('...', SUBSTRING(v_quoted_table, -13)), v_quoted_table),
                                             ' does not exist.');
                    SIGNAL SQLSTATE '45000'
                       SET MESSAGE_TEXT = v_error_msg;
                END IF;
            END IF;
        WHEN in_action IN ('delta', 'save') THEN
            IF (v_table_exists NOT IN ('TEMPORARY', 'BASE TABLE')) THEN
                SET v_error_msg = CONCAT('The ', in_action, ' action requires in_table to be an existing table.',
                                         IF(in_table IS NOT NULL, CONCAT(' The table ',
                                             IF(CHAR_LENGTH(v_quoted_table) > 39, CONCAT('...', SUBSTRING(v_quoted_table, -36)), v_quoted_table),
                                             ' does not exist.'), ''));
                SIGNAL SQLSTATE '45000'
                   SET MESSAGE_TEXT = v_error_msg;
            END IF;
            IF (in_action = 'delta' AND v_tmp_digests_table_exists <> 'TEMPORARY') THEN
                SIGNAL SQLSTATE '45000'
                   SET MESSAGE_TEXT = 'An existing snapshot generated with the statement_performance_analyzer() must exist.';
            END IF;
        WHEN in_action = 'create_tmp' THEN
            IF (v_table_exists = 'TEMPORARY') THEN
                SET v_error_msg = CONCAT('Cannot create the table ',
                                         IF(CHAR_LENGTH(v_quoted_table) > 72, CONCAT('...', SUBSTRING(v_quoted_table, -69)), v_quoted_table),
                                         ' as it already exists.');
                SIGNAL SQLSTATE '45000'
                   SET MESSAGE_TEXT = v_error_msg;
            END IF;
        WHEN in_action = 'create_table' THEN
            IF (v_table_exists <> '') THEN
                SET v_error_msg = CONCAT('Cannot create the table ',
                                         IF(CHAR_LENGTH(v_quoted_table) > 52, CONCAT('...', SUBSTRING(v_quoted_table, -49)), v_quoted_table),
                                         ' as it already exists',
                                         IF(v_table_exists = 'TEMPORARY', ' as a temporary table.', '.'));
                SIGNAL SQLSTATE '45000'
                   SET MESSAGE_TEXT = v_error_msg;
            END IF;
        WHEN in_action = 'cleanup' THEN
            DO (SELECT 1);
        ELSE
            SIGNAL SQLSTATE '45000'
               SET MESSAGE_TEXT = 'Unknown action. Supported actions are: cleanup, create_table, create_tmp, delta, overall, save, snapshot';
    END CASE;
    SET v_digest_table_template = 'CREATE %{TEMPORARY}TABLE %{TABLE_NAME} (\
  `SCHEMA_NAME` varchar(64) DEFAULT NULL,\
  `DIGEST` varchar(32) DEFAULT NULL,\
  `DIGEST_TEXT` longtext,\
  `COUNT_STAR` bigint(20) unsigned NOT NULL,\
  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\
  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\
  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\
  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\
  `SUM_LOCK_TIME` bigint(20) unsigned NOT NULL,\
  `SUM_ERRORS` bigint(20) unsigned NOT NULL,\
  `SUM_WARNINGS` bigint(20) unsigned NOT NULL,\
  `SUM_ROWS_AFFECTED` bigint(20) unsigned NOT NULL,\
  `SUM_ROWS_SENT` bigint(20) unsigned NOT NULL,\
  `SUM_ROWS_EXAMINED` bigint(20) unsigned NOT NULL,\
  `SUM_CREATED_TMP_DISK_TABLES` bigint(20) unsigned NOT NULL,\
  `SUM_CREATED_TMP_TABLES` bigint(20) unsigned NOT NULL,\
  `SUM_SELECT_FULL_JOIN` bigint(20) unsigned NOT NULL,\
  `SUM_SELECT_FULL_RANGE_JOIN` bigint(20) unsigned NOT NULL,\
  `SUM_SELECT_RANGE` bigint(20) unsigned NOT NULL,\
  `SUM_SELECT_RANGE_CHECK` bigint(20) unsigned NOT NULL,\
  `SUM_SELECT_SCAN` bigint(20) unsigned NOT NULL,\
  `SUM_SORT_MERGE_PASSES` bigint(20) unsigned NOT NULL,\
  `SUM_SORT_RANGE` bigint(20) unsigned NOT NULL,\
  `SUM_SORT_ROWS` bigint(20) unsigned NOT NULL,\
  `SUM_SORT_SCAN` bigint(20) unsigned NOT NULL,\
  `SUM_NO_INDEX_USED` bigint(20) unsigned NOT NULL,\
  `SUM_NO_GOOD_INDEX_USED` bigint(20) unsigned NOT NULL,\
  `FIRST_SEEN` timestamp NULL DEFAULT NULL,\
  `LAST_SEEN` timestamp NULL DEFAULT NULL,\
  INDEX (SCHEMA_NAME, DIGEST)\
) DEFAULT CHARSET=utf8';
    IF (v_force_new_snapshot
           OR in_action = 'snapshot'
           OR (in_action = 'overall' AND in_table IS NULL)
           OR (in_action = 'save' AND v_tmp_digests_table_exists <> 'TEMPORARY')
       ) THEN
        IF (v_tmp_digests_table_exists = 'TEMPORARY') THEN
            IF (@sys.debug = 'ON') THEN
                SELECT 'DROP TEMPORARY TABLE IF EXISTS tmp_digests' AS 'Debug';
            END IF;
            DROP TEMPORARY TABLE IF EXISTS tmp_digests;
        END IF;
        CALL sys.execute_prepared_stmt(REPLACE(REPLACE(v_digest_table_template, '%{TEMPORARY}', 'TEMPORARY '), '%{TABLE_NAME}', 'tmp_digests'));
        SET v_sql = CONCAT('INSERT INTO tmp_digests SELECT * FROM ',
                           IF(in_table IS NULL OR in_action = 'save', 'performance_schema.events_statements_summary_by_digest', v_quoted_table));
        CALL sys.execute_prepared_stmt(v_sql);
    END IF;
    IF (in_action IN ('create_table', 'create_tmp')) THEN
        IF (in_action = 'create_table') THEN
            CALL sys.execute_prepared_stmt(REPLACE(REPLACE(v_digest_table_template, '%{TEMPORARY}', ''), '%{TABLE_NAME}', v_quoted_table));
        ELSE
            CALL sys.execute_prepared_stmt(REPLACE(REPLACE(v_digest_table_template, '%{TEMPORARY}', 'TEMPORARY '), '%{TABLE_NAME}', v_quoted_table));
        END IF;
    ELSEIF (in_action = 'save') THEN
        CALL sys.execute_prepared_stmt(CONCAT('DELETE FROM ', v_quoted_table));
        CALL sys.execute_prepared_stmt(CONCAT('INSERT INTO ', v_quoted_table, ' SELECT * FROM tmp_digests'));
    ELSEIF (in_action = 'cleanup') THEN
        DROP TEMPORARY TABLE IF EXISTS sys.tmp_digests;
        DROP TEMPORARY TABLE IF EXISTS sys.tmp_digests_delta;
    ELSEIF (in_action IN ('overall', 'delta')) THEN
        IF (in_action = 'overall') THEN
            IF (in_table IS NULL) THEN
                SET v_digests_table = 'tmp_digests';
            ELSE
                SET v_digests_table = v_quoted_table;
            END IF;
        ELSE
            SET v_digests_table = 'tmp_digests_delta';
            DROP TEMPORARY TABLE IF EXISTS tmp_digests_delta;
            CREATE TEMPORARY TABLE tmp_digests_delta LIKE tmp_digests;
            SET v_sql = CONCAT('INSERT INTO tmp_digests_delta\
SELECT `d_end`.`SCHEMA_NAME`,\
       `d_end`.`DIGEST`,\
       `d_end`.`DIGEST_TEXT`,\
       `d_end`.`COUNT_STAR`-IFNULL(`d_start`.`COUNT_STAR`, 0) AS ''COUNT_STAR'',\
       `d_end`.`SUM_TIMER_WAIT`-IFNULL(`d_start`.`SUM_TIMER_WAIT`, 0) AS ''SUM_TIMER_WAIT'',\
       `d_end`.`MIN_TIMER_WAIT` AS ''MIN_TIMER_WAIT'',\
       IFNULL((`d_end`.`SUM_TIMER_WAIT`-IFNULL(`d_start`.`SUM_TIMER_WAIT`, 0))/NULLIF(`d_end`.`COUNT_STAR`-IFNULL(`d_start`.`COUNT_STAR`, 0), 0), 0) AS ''AVG_TIMER_WAIT'',\
       `d_end`.`MAX_TIMER_WAIT` AS ''MAX_TIMER_WAIT'',\
       `d_end`.`SUM_LOCK_TIME`-IFNULL(`d_start`.`SUM_LOCK_TIME`, 0) AS ''SUM_LOCK_TIME'',\
       `d_end`.`SUM_ERRORS`-IFNULL(`d_start`.`SUM_ERRORS`, 0) AS ''SUM_ERRORS'',\
       `d_end`.`SUM_WARNINGS`-IFNULL(`d_start`.`SUM_WARNINGS`, 0) AS ''SUM_WARNINGS'',\
       `d_end`.`SUM_ROWS_AFFECTED`-IFNULL(`d_start`.`SUM_ROWS_AFFECTED`, 0) AS ''SUM_ROWS_AFFECTED'',\
       `d_end`.`SUM_ROWS_SENT`-IFNULL(`d_start`.`SUM_ROWS_SENT`, 0) AS ''SUM_ROWS_SENT'',\
       `d_end`.`SUM_ROWS_EXAMINED`-IFNULL(`d_start`.`SUM_ROWS_EXAMINED`, 0) AS ''SUM_ROWS_EXAMINED'',\
       `d_end`.`SUM_CREATED_TMP_DISK_TABLES`-IFNULL(`d_start`.`SUM_CREATED_TMP_DISK_TABLES`, 0) AS ''SUM_CREATED_TMP_DISK_TABLES'',\
       `d_end`.`SUM_CREATED_TMP_TABLES`-IFNULL(`d_start`.`SUM_CREATED_TMP_TABLES`, 0) AS ''SUM_CREATED_TMP_TABLES'',\
       `d_end`.`SUM_SELECT_FULL_JOIN`-IFNULL(`d_start`.`SUM_SELECT_FULL_JOIN`, 0) AS ''SUM_SELECT_FULL_JOIN'',\
       `d_end`.`SUM_SELECT_FULL_RANGE_JOIN`-IFNULL(`d_start`.`SUM_SELECT_FULL_RANGE_JOIN`, 0) AS ''SUM_SELECT_FULL_RANGE_JOIN'',\
       `d_end`.`SUM_SELECT_RANGE`-IFNULL(`d_start`.`SUM_SELECT_RANGE`, 0) AS ''SUM_SELECT_RANGE'',\
       `d_end`.`SUM_SELECT_RANGE_CHECK`-IFNULL(`d_start`.`SUM_SELECT_RANGE_CHECK`, 0) AS ''SUM_SELECT_RANGE_CHECK'',\
       `d_end`.`SUM_SELECT_SCAN`-IFNULL(`d_start`.`SUM_SELECT_SCAN`, 0) AS ''SUM_SELECT_SCAN'',\
       `d_end`.`SUM_SORT_MERGE_PASSES`-IFNULL(`d_start`.`SUM_SORT_MERGE_PASSES`, 0) AS ''SUM_SORT_MERGE_PASSES'',\
       `d_end`.`SUM_SORT_RANGE`-IFNULL(`d_start`.`SUM_SORT_RANGE`, 0) AS ''SUM_SORT_RANGE'',\
       `d_end`.`SUM_SORT_ROWS`-IFNULL(`d_start`.`SUM_SORT_ROWS`, 0) AS ''SUM_SORT_ROWS'',\
       `d_end`.`SUM_SORT_SCAN`-IFNULL(`d_start`.`SUM_SORT_SCAN`, 0) AS ''SUM_SORT_SCAN'',\
       `d_end`.`SUM_NO_INDEX_USED`-IFNULL(`d_start`.`SUM_NO_INDEX_USED`, 0) AS ''SUM_NO_INDEX_USED'',\
       `d_end`.`SUM_NO_GOOD_INDEX_USED`-IFNULL(`d_start`.`SUM_NO_GOOD_INDEX_USED`, 0) AS ''SUM_NO_GOOD_INDEX_USED'',\
       `d_end`.`FIRST_SEEN`,\
       `d_end`.`LAST_SEEN`\
  FROM tmp_digests d_end\
       LEFT OUTER JOIN ', v_quoted_table, ' d_start ON `d_start`.`DIGEST` = `d_end`.`DIGEST`\
                                                    AND (`d_start`.`SCHEMA_NAME` = `d_end`.`SCHEMA_NAME`\
                                                          OR (`d_start`.`SCHEMA_NAME` IS NULL AND `d_end`.`SCHEMA_NAME` IS NULL)\
                                                        )\
 WHERE `d_end`.`COUNT_STAR`-IFNULL(`d_start`.`COUNT_STAR`, 0) > 0');
            CALL sys.execute_prepared_stmt(v_sql);
        END IF;
        IF (FIND_IN_SET('with_runtimes_in_95th_percentile', in_views)) THEN
            SELECT 'Queries with Runtime in 95th Percentile' AS 'Next Output';
            DROP TEMPORARY TABLE IF EXISTS tmp_digest_avg_latency_distribution1;
            DROP TEMPORARY TABLE IF EXISTS tmp_digest_avg_latency_distribution2;
            DROP TEMPORARY TABLE IF EXISTS tmp_digest_95th_percentile_by_avg_us;
            CREATE TEMPORARY TABLE tmp_digest_avg_latency_distribution1 (
              cnt bigint unsigned NOT NULL,
              avg_us decimal(21,0) NOT NULL,
              PRIMARY KEY (avg_us)
            ) ENGINE=InnoDB;
            SET v_sql = CONCAT('INSERT INTO tmp_digest_avg_latency_distribution1\
SELECT COUNT(*) cnt,\
       ROUND(avg_timer_wait/1000000) AS avg_us\
  FROM ', v_digests_table, '\
 GROUP BY avg_us');
            CALL sys.execute_prepared_stmt(v_sql);
            CREATE TEMPORARY TABLE tmp_digest_avg_latency_distribution2 LIKE tmp_digest_avg_latency_distribution1;
            INSERT INTO tmp_digest_avg_latency_distribution2 SELECT * FROM tmp_digest_avg_latency_distribution1;
            CREATE TEMPORARY TABLE tmp_digest_95th_percentile_by_avg_us (
              avg_us decimal(21,0) NOT NULL,
              percentile decimal(46,4) NOT NULL,
              PRIMARY KEY (avg_us)
            ) ENGINE=InnoDB;
            SET v_sql = CONCAT('INSERT INTO tmp_digest_95th_percentile_by_avg_us\
SELECT s2.avg_us avg_us,\
       IFNULL(SUM(s1.cnt)/NULLIF((SELECT COUNT(*) FROM ', v_digests_table, '), 0), 0) percentile\
  FROM tmp_digest_avg_latency_distribution1 AS s1\
       JOIN tmp_digest_avg_latency_distribution2 AS s2 ON s1.avg_us <= s2.avg_us\
 GROUP BY s2.avg_us\
HAVING percentile > 0.95\
 ORDER BY percentile\
 LIMIT 1');
            CALL sys.execute_prepared_stmt(v_sql);
            SET v_sql =
                REPLACE(
                    REPLACE(
                        (SELECT VIEW_DEFINITION
                           FROM information_schema.VIEWS
                          WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'statements_with_runtimes_in_95th_percentile'
                        ),
                        '`performance_schema`.`events_statements_summary_by_digest`',
                        v_digests_table
                    ),
                    'sys.x$ps_digest_95th_percentile_by_avg_us',
                    '`sys`.`x$ps_digest_95th_percentile_by_avg_us`'
              );
            CALL sys.execute_prepared_stmt(v_sql);
            DROP TEMPORARY TABLE tmp_digest_avg_latency_distribution1;
            DROP TEMPORARY TABLE tmp_digest_avg_latency_distribution2;
            DROP TEMPORARY TABLE tmp_digest_95th_percentile_by_avg_us;
        END IF;
        IF (FIND_IN_SET('analysis', in_views)) THEN
            SELECT CONCAT('Top ', @sys.statement_performance_analyzer.limit, ' Queries Ordered by Total Latency') AS 'Next Output';
            SET v_sql =
                REPLACE(
                    (SELECT VIEW_DEFINITION
                       FROM information_schema.VIEWS
                      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'statement_analysis'
                    ),
                    '`performance_schema`.`events_statements_summary_by_digest`',
                    v_digests_table
                );
            IF (@sys.statement_performance_analyzer.limit > 0) THEN
                SET v_sql = CONCAT(v_sql, ' LIMIT ', @sys.statement_performance_analyzer.limit);
            END IF;
            CALL sys.execute_prepared_stmt(v_sql);
        END IF;
        IF (FIND_IN_SET('with_errors_or_warnings', in_views)) THEN
            SELECT CONCAT('Top ', @sys.statement_performance_analyzer.limit, ' Queries with Errors') AS 'Next Output';
            SET v_sql =
                REPLACE(
                    (SELECT VIEW_DEFINITION
                       FROM information_schema.VIEWS
                      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'statements_with_errors_or_warnings'
                    ),
                    '`performance_schema`.`events_statements_summary_by_digest`',
                    v_digests_table
                );
            IF (@sys.statement_performance_analyzer.limit > 0) THEN
                SET v_sql = CONCAT(v_sql, ' LIMIT ', @sys.statement_performance_analyzer.limit);
            END IF;
            CALL sys.execute_prepared_stmt(v_sql);
        END IF;
        IF (FIND_IN_SET('with_full_table_scans', in_views)) THEN
            SELECT CONCAT('Top ', @sys.statement_performance_analyzer.limit, ' Queries with Full Table Scan') AS 'Next Output';
            SET v_sql =
                REPLACE(
                    (SELECT VIEW_DEFINITION
                       FROM information_schema.VIEWS
                      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'statements_with_full_table_scans'
                    ),
                    '`performance_schema`.`events_statements_summary_by_digest`',
                    v_digests_table
                );
            IF (@sys.statement_performance_analyzer.limit > 0) THEN
                SET v_sql = CONCAT(v_sql, ' LIMIT ', @sys.statement_performance_analyzer.limit);
            END IF;
            CALL sys.execute_prepared_stmt(v_sql);
        END IF;
        IF (FIND_IN_SET('with_sorting', in_views)) THEN
            SELECT CONCAT('Top ', @sys.statement_performance_analyzer.limit, ' Queries with Sorting') AS 'Next Output';
            SET v_sql =
                REPLACE(
                    (SELECT VIEW_DEFINITION
                       FROM information_schema.VIEWS
                      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'statements_with_sorting'
                    ),
                    '`performance_schema`.`events_statements_summary_by_digest`',
                    v_digests_table
                );
            IF (@sys.statement_performance_analyzer.limit > 0) THEN
                SET v_sql = CONCAT(v_sql, ' LIMIT ', @sys.statement_performance_analyzer.limit);
            END IF;
            CALL sys.execute_prepared_stmt(v_sql);
        END IF;
        IF (FIND_IN_SET('with_temp_tables', in_views)) THEN
            SELECT CONCAT('Top ', @sys.statement_performance_analyzer.limit, ' Queries with Internal Temporary Tables') AS 'Next Output';
            SET v_sql =
                REPLACE(
                    (SELECT VIEW_DEFINITION
                       FROM information_schema.VIEWS
                      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'statements_with_temp_tables'
                    ),
                    '`performance_schema`.`events_statements_summary_by_digest`',
                    v_digests_table
                );
            IF (@sys.statement_performance_analyzer.limit > 0) THEN
                SET v_sql = CONCAT(v_sql, ' LIMIT ', @sys.statement_performance_analyzer.limit);
            END IF;
            CALL sys.execute_prepared_stmt(v_sql);
        END IF;
        IF (FIND_IN_SET('custom', in_views)) THEN
            SELECT CONCAT('Top ', @sys.statement_performance_analyzer.limit, ' Queries Using Custom View') AS 'Next Output';
            IF (@sys.statement_performance_analyzer.view IS NULL) THEN
                SET @sys.statement_performance_analyzer.view = sys.sys_get_config('statement_performance_analyzer.view', NULL);
            END IF;
            IF (@sys.statement_performance_analyzer.view IS NULL) THEN
                SIGNAL SQLSTATE '45000'
                   SET MESSAGE_TEXT = 'The @sys.statement_performance_analyzer.view user variable must be set with the view or query to use.';
            END IF;
            IF (NOT INSTR(@sys.statement_performance_analyzer.view, ' ')) THEN
                IF (NOT INSTR(@sys.statement_performance_analyzer.view, '.')) THEN
                    SET v_custom_db   = DATABASE(),
                        v_custom_name = @sys.statement_performance_analyzer.view;
                ELSE
                    SET v_custom_db   = SUBSTRING_INDEX(@sys.statement_performance_analyzer.view, '.', 1);
                    SET v_custom_name = SUBSTRING(@sys.statement_performance_analyzer.view, CHAR_LENGTH(v_custom_db)+2);
                END IF;
                CALL sys.table_exists(v_custom_db, v_custom_name, v_custom_view_exists);
                IF (v_custom_view_exists <> 'VIEW') THEN
                    SIGNAL SQLSTATE '45000'
                       SET MESSAGE_TEXT = 'The @sys.statement_performance_analyzer.view user variable is set but specified neither an existing view nor a query.';
                END IF;
                SET v_sql =
                    REPLACE(
                        (SELECT VIEW_DEFINITION
                           FROM information_schema.VIEWS
                          WHERE TABLE_SCHEMA = v_custom_db AND TABLE_NAME = v_custom_name
                        ),
                        '`performance_schema`.`events_statements_summary_by_digest`',
                        v_digests_table
                    );
            ELSE
                SET v_sql = REPLACE(@sys.statement_performance_analyzer.view, '`performance_schema`.`events_statements_summary_by_digest`', v_digests_table);
            END IF;
            IF (@sys.statement_performance_analyzer.limit > 0) THEN
                SET v_sql = CONCAT(v_sql, ' LIMIT ', @sys.statement_performance_analyzer.limit);
            END IF;
            CALL sys.execute_prepared_stmt(v_sql);
        END IF;
    END IF;
    IF (v_this_thread_enabled = 'YES') THEN
        CALL sys.ps_setup_enable_thread(CONNECTION_ID());
    END IF;
    IF (@log_bin = 1) THEN
        SET sql_log_bin = @log_bin;
    END IF;
END
sys_get_config
BEGIN
    DECLARE v_value VARCHAR(128) DEFAULT NULL;
    DECLARE old_val INTEGER DEFAULT NULL;
    SET v_value = (SELECT value FROM sys.sys_config WHERE variable = in_variable_name);
    IF (v_value IS NULL) THEN
        SET v_value = in_default_value;
    END IF;
    RETURN v_value;
END
version_major
BEGIN
    RETURN SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '-', 1), '.', 1);
END
version_minor
BEGIN
    RETURN SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '-', 1), '.', 2), '.', -1);
END
version_patch
BEGIN
    RETURN SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '-', 1), '.', -1);
END
SLEV
BEGIN 
        DECLARE st_o_id     INTEGER;
        DECLARE `Constraint Violation` CONDITION FOR SQLSTATE '23000';
        DECLARE EXIT HANDLER FOR `Constraint Violation` ROLLBACK;
        DECLARE EXIT HANDLER FOR NOT FOUND ROLLBACK;
        START TRANSACTION;
        SELECT d_next_o_id INTO st_o_id
        FROM district
        WHERE d_w_id=st_w_id AND d_id=st_d_id;
        SELECT COUNT(DISTINCT (s_i_id)) INTO stock_count
        FROM order_line, stock
        WHERE ol_w_id = st_w_id AND
        ol_d_id = st_d_id AND (ol_o_id < st_o_id) AND
        ol_o_id >= (st_o_id - 20) AND s_w_id = st_w_id AND
        s_i_id = ol_i_id AND s_quantity < threshold;
        COMMIT;
        END
ps_setup_save
BEGIN
    DECLARE v_lock_result INT;
    SET @log_bin := @@sql_log_bin;
    SET sql_log_bin = 0;
    SELECT GET_LOCK('sys.ps_setup_save', in_timeout) INTO v_lock_result;
    IF v_lock_result THEN
        DROP TEMPORARY TABLE IF EXISTS tmp_setup_actors;
        DROP TEMPORARY TABLE IF EXISTS tmp_setup_consumers;
        DROP TEMPORARY TABLE IF EXISTS tmp_setup_instruments;
        DROP TEMPORARY TABLE IF EXISTS tmp_threads;
        CREATE TEMPORARY TABLE tmp_setup_actors AS SELECT * FROM performance_schema.setup_actors;
        CREATE TEMPORARY TABLE tmp_setup_consumers AS SELECT * FROM  performance_schema.setup_consumers;
        CREATE TEMPORARY TABLE tmp_setup_instruments AS SELECT * FROM  performance_schema.setup_instruments;
        CREATE TEMPORARY TABLE tmp_threads (THREAD_ID bigint unsigned NOT NULL PRIMARY KEY, INSTRUMENTED enum('YES','NO') NOT NULL);
        INSERT INTO tmp_threads SELECT THREAD_ID, INSTRUMENTED FROM performance_schema.threads;
    ELSE
        SIGNAL SQLSTATE VALUE '90000'
           SET MESSAGE_TEXT = 'Could not lock the sys.ps_setup_save user lock, another thread has a saved configuration';
    END IF;
    SET sql_log_bin = @log_bin;
END
ps_setup_show_disabled
BEGIN
    SELECT @@performance_schema AS performance_schema_enabled;
    
    SELECT object_type,
           CONCAT(object_schema, '.', object_name) AS objects,
           enabled,
           timed
      FROM performance_schema.setup_objects
     WHERE enabled = 'NO'
     ORDER BY object_type, objects;
    SELECT name AS disabled_consumers
      FROM performance_schema.setup_consumers
     WHERE enabled = 'NO'
     ORDER BY disabled_consumers;
    IF (in_show_threads) THEN
        SELECT IF(name = 'thread/sql/one_connection',
                  CONCAT(processlist_user, '@', processlist_host),
                  REPLACE(name, 'thread/', '')) AS disabled_threads,
        TYPE AS thread_type
          FROM performance_schema.threads
         WHERE INSTRUMENTED = 'NO'
         ORDER BY disabled_threads;
    END IF;
    IF (in_show_instruments) THEN
        SELECT name AS disabled_instruments,
               timed
          FROM performance_schema.setup_instruments
         WHERE enabled = 'NO'
         ORDER BY disabled_instruments;
    END IF;
END
ps_setup_show_enabled
BEGIN
    SELECT @@performance_schema AS performance_schema_enabled;
    SELECT CONCAT('''', user, '''@''', host, '''') AS enabled_users
      FROM performance_schema.setup_actors
      WHERE enabled = 'YES'
     ORDER BY enabled_users;
    SELECT object_type,
           CONCAT(object_schema, '.', object_name) AS objects,
           enabled,
           timed
      FROM performance_schema.setup_objects
     WHERE enabled = 'YES'
     ORDER BY object_type, objects;
    SELECT name AS enabled_consumers
      FROM performance_schema.setup_consumers
     WHERE enabled = 'YES'
     ORDER BY enabled_consumers;
    IF (in_show_threads) THEN
        SELECT IF(name = 'thread/sql/one_connection',
                  CONCAT(processlist_user, '@', processlist_host),
                  REPLACE(name, 'thread/', '')) AS enabled_threads,
        TYPE AS thread_type
          FROM performance_schema.threads
         WHERE INSTRUMENTED = 'YES' AND name <> 'thread/innodb/thread_pool_thread'
         ORDER BY enabled_threads;
    END IF;
    IF (in_show_instruments) THEN
        SELECT name AS enabled_instruments,
               timed
          FROM performance_schema.setup_instruments
         WHERE enabled = 'YES'
         ORDER BY enabled_instruments;
    END IF;
END
ps_truncate_all_tables
BEGIN
    DECLARE v_done INT DEFAULT FALSE;
    DECLARE v_total_tables INT DEFAULT 0;
    DECLARE v_ps_table VARCHAR(64);
    DECLARE ps_tables CURSOR FOR
        SELECT table_name
          FROM INFORMATION_SCHEMA.TABLES
         WHERE table_schema = 'performance_schema'
           AND (table_name LIKE '%summary%'
            OR table_name LIKE '%history%');
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;
    OPEN ps_tables;
    ps_tables_loop: LOOP
        FETCH ps_tables INTO v_ps_table;
        IF v_done THEN
          LEAVE ps_tables_loop;
        END IF;
        SET @truncate_stmt := CONCAT('TRUNCATE TABLE performance_schema.', v_ps_table);
        IF in_verbose THEN
            SELECT CONCAT('Running: ', @truncate_stmt) AS status;
        END IF;
        PREPARE truncate_stmt FROM @truncate_stmt;
        EXECUTE truncate_stmt;
        DEALLOCATE PREPARE truncate_stmt;
        SET v_total_tables = v_total_tables + 1;
    END LOOP;
    CLOSE ps_tables;
    SELECT CONCAT('Truncated ', v_total_tables, ' tables') AS summary;
END
table_exists
BEGIN
    DECLARE v_error BOOLEAN DEFAULT FALSE;
    DECLARE db_quoted VARCHAR(64);
    DECLARE table_quoted VARCHAR(64);
    DECLARE v_table_type VARCHAR(30) DEFAULT '';
    DECLARE CONTINUE HANDLER FOR 1050 SET v_error = TRUE;
    DECLARE CONTINUE HANDLER FOR 1146 SET v_error = TRUE;
    SET v_table_type = (SELECT GROUP_CONCAT(TABLE_TYPE) FROM information_schema.TABLES WHERE
                            TABLE_SCHEMA = in_db AND TABLE_NAME = in_table);
    IF v_table_type LIKE '%,%' THEN
        SET out_exists = 'TEMPORARY';
    ELSE
        IF v_table_type is NULL
        THEN
            SET v_table_type='';
        END IF;
        IF v_table_type = 'SYSTEM VERSIONED' THEN
            SET out_exists = 'BASE TABLE';
        ELSE
            SET out_exists = v_table_type;
        END IF;
    END IF;
END
ps_thread_stack
BEGIN
    DECLARE json_objects LONGTEXT;
    
    UPDATE performance_schema.threads
       SET instrumented = 'NO'
     WHERE processlist_id = CONNECTION_ID();
    
    SET SESSION group_concat_max_len=@@global.max_allowed_packet;
    SELECT GROUP_CONCAT(CONCAT( '{'
              , CONCAT_WS( ', '
              , CONCAT('"nesting_event_id": "', IF(nesting_event_id IS NULL, '0', nesting_event_id), '"')
              , CONCAT('"event_id": "', event_id, '"')
              , CONCAT( '"timer_wait": ', ROUND(timer_wait/1000000, 2))
              , CONCAT( '"event_info": "'
                  , CASE
                        WHEN event_name NOT LIKE 'wait/io%' THEN REPLACE(SUBSTRING_INDEX(event_name, '/', -2), '\\', '\\\\')
                        WHEN event_name NOT LIKE 'wait/io/file%' OR event_name NOT LIKE 'wait/io/socket%' THEN REPLACE(SUBSTRING_INDEX(event_name, '/', -4), '\\', '\\\\')
                        ELSE event_name
                    END
                  , '"'
              )
              , CONCAT( '"wait_info": "', IFNULL(wait_info, ''), '"')
              , CONCAT( '"source": "', IF(true AND event_name LIKE 'wait%', IFNULL(wait_info, ''), ''), '"')
              , CASE
                     WHEN event_name LIKE 'wait/io/file%'      THEN '"event_type": "io/file"'
                     WHEN event_name LIKE 'wait/io/table%'     THEN '"event_type": "io/table"'
                     WHEN event_name LIKE 'wait/io/socket%'    THEN '"event_type": "io/socket"'
                     WHEN event_name LIKE 'wait/synch/mutex%'  THEN '"event_type": "synch/mutex"'
                     WHEN event_name LIKE 'wait/synch/cond%'   THEN '"event_type": "synch/cond"'
                     WHEN event_name LIKE 'wait/synch/rwlock%' THEN '"event_type": "synch/rwlock"'
                     WHEN event_name LIKE 'wait/lock%'         THEN '"event_type": "lock"'
                     WHEN event_name LIKE 'statement/%'        THEN '"event_type": "stmt"'
                     WHEN event_name LIKE 'stage/%'            THEN '"event_type": "stage"'
                     WHEN event_name LIKE '%idle%'             THEN '"event_type": "idle"'
                     ELSE ''
                END
            )
            , '}'
          )
          ORDER BY event_id ASC SEPARATOR ',') event
    INTO json_objects
    FROM (
          
          (SELECT thread_id, event_id, event_name, timer_wait, timer_start, nesting_event_id,
                  CONCAT(sql_text, '\
',
                         'errors: ', errors, '\
',
                         'warnings: ', warnings, '\
',
                         'lock time: ', ROUND(lock_time/1000000, 2),'us\
',
                         'rows affected: ', rows_affected, '\
',
                         'rows sent: ', rows_sent, '\
',
                         'rows examined: ', rows_examined, '\
',
                         'tmp tables: ', created_tmp_tables, '\
',
                         'tmp disk tables: ', created_tmp_disk_tables, '\
',
                         'select scan: ', select_scan, '\
',
                         'select full join: ', select_full_join, '\
',
                         'select full range join: ', select_full_range_join, '\
',
                         'select range: ', select_range, '\
',
                         'select range check: ', select_range_check, '\
',
                         'sort merge passes: ', sort_merge_passes, '\
',
                         'sort rows: ', sort_rows, '\
',
                         'sort range: ', sort_range, '\
',
                         'sort scan: ', sort_scan, '\
',
                         'no index used: ', IF(no_index_used, 'TRUE', 'FALSE'), '\
',
                         'no good index used: ', IF(no_good_index_used, 'TRUE', 'FALSE'), '\
'
                         ) AS wait_info
             FROM performance_schema.events_statements_history_long WHERE thread_id = thd_id)
          UNION
          (SELECT thread_id, event_id, event_name, timer_wait, timer_start, nesting_event_id, null AS wait_info
             FROM performance_schema.events_stages_history_long WHERE thread_id = thd_id)
          UNION 
          (SELECT thread_id, event_id,
                  CONCAT(event_name ,
                         IF(event_name NOT LIKE 'wait/synch/mutex%', IFNULL(CONCAT(' - ', operation), ''), ''),
                         IF(number_of_bytes IS NOT NULL, CONCAT(' ', number_of_bytes, ' bytes'), ''),
                         IF(event_name LIKE 'wait/io/file%', '\
', ''),
                         IF(object_schema IS NOT NULL, CONCAT('\
Object: ', object_schema, '.'), ''),
                         IF(object_name IS NOT NULL,
                            IF (event_name LIKE 'wait/io/socket%',
                                CONCAT(IF (object_name LIKE ':0%', @@socket, object_name)),
                                object_name),
                            ''),
                          IF(index_name IS NOT NULL, CONCAT(' Index: ', index_name), ''),'\
'
                         ) AS event_name,
                  timer_wait, timer_start, nesting_event_id, source AS wait_info
             FROM performance_schema.events_waits_history_long WHERE thread_id = thd_id)) events
    ORDER BY event_id;
    RETURN CONCAT('{',
                  CONCAT_WS(',',
                            '"rankdir": "LR"',
                            '"nodesep": "0.10"',
                            CONCAT('"stack_created": "', NOW(), '"'),
                            CONCAT('"mysql_version": "', VERSION(), '"'),
                            CONCAT('"mysql_user": "', CURRENT_USER(), '"'),
                            CONCAT('"events": [', IFNULL(json_objects,''), ']')
                           ),
                  '}');
END
ps_thread_trx_info
BEGIN
    DECLARE v_output LONGTEXT DEFAULT '{}';
    DECLARE v_msg_text TEXT DEFAULT '';
    DECLARE v_signal_msg TEXT DEFAULT '';
    DECLARE v_mysql_errno INT;
    DECLARE v_max_output_len BIGINT;
    DECLARE EXIT HANDLER FOR SQLWARNING, SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            v_msg_text = MESSAGE_TEXT,
            v_mysql_errno = MYSQL_ERRNO;
        IF v_mysql_errno = 1260 THEN
            SET v_signal_msg = CONCAT('{ "error": "Trx info truncated: ', v_msg_text, '" }');
        ELSE
            SET v_signal_msg = CONCAT('{ "error": "', v_msg_text, '" }');
        END IF;
        RETURN v_signal_msg;
    END;
    IF (@sys.ps_thread_trx_info.max_length IS NULL) THEN
        SET @sys.ps_thread_trx_info.max_length = sys.sys_get_config('ps_thread_trx_info.max_length', 65535);
    END IF;
    IF (@sys.ps_thread_trx_info.max_length != @@session.group_concat_max_len) THEN
        SET @old_group_concat_max_len = @@session.group_concat_max_len;
        SET v_max_output_len = (@sys.ps_thread_trx_info.max_length - 5);
        SET SESSION group_concat_max_len = v_max_output_len;
    END IF;
    SET v_output = (
        SELECT CONCAT('[', IFNULL(GROUP_CONCAT(trx_info ORDER BY event_id), ''), '\
]') AS trx_info
          FROM (SELECT trxi.thread_id,
                       trxi.event_id,
                       GROUP_CONCAT(
                         IFNULL(
                           CONCAT('\
  {\
',
                                  '    "time": "', IFNULL(format_pico_time(trxi.timer_wait), ''), '",\
',
                                  '    "state": "', IFNULL(trxi.state, ''), '",\
',
                                  '    "mode": "', IFNULL(trxi.access_mode, ''), '",\
',
                                  '    "autocommitted": "', IFNULL(trxi.autocommit, ''), '",\
',
                                  '    "gtid": "', IFNULL(trxi.gtid, ''), '",\
',
                                  '    "isolation": "', IFNULL(trxi.isolation_level, ''), '",\
',
                                  '    "statements_executed": [', IFNULL(s.stmts, ''), IF(s.stmts IS NULL, ' ]\
', '\
    ]\
'),
                                  '  }'
                           ),
                           '')
                         ORDER BY event_id) AS trx_info
                  FROM (
                        (SELECT thread_id, event_id, timer_wait, state,access_mode, autocommit, gtid, isolation_level
                           FROM performance_schema.events_transactions_current
                          WHERE thread_id = in_thread_id
                            AND end_event_id IS NULL)
                        UNION
                        (SELECT thread_id, event_id, timer_wait, state,access_mode, autocommit, gtid, isolation_level
                           FROM performance_schema.events_transactions_history
                          WHERE thread_id = in_thread_id)
                       ) AS trxi
                  LEFT JOIN (SELECT thread_id,
                                    nesting_event_id,
                                    GROUP_CONCAT(
                                      IFNULL(
                                        CONCAT('\
      {\
',
                                               '        "sql_text": "', IFNULL(sys.format_statement(REPLACE(sql_text, '\\', '\\\\')), ''), '",\
',
                                               '        "time": "', IFNULL(format_pico_time(timer_wait), ''), '",\
',
                                               '        "schema": "', IFNULL(current_schema, ''), '",\
',
                                               '        "rows_examined": ', IFNULL(rows_examined, ''), ',\
',
                                               '        "rows_affected": ', IFNULL(rows_affected, ''), ',\
',
                                               '        "rows_sent": ', IFNULL(rows_sent, ''), ',\
',
                                               '        "tmp_tables": ', IFNULL(created_tmp_tables, ''), ',\
',
                                               '        "tmp_disk_tables": ', IFNULL(created_tmp_disk_tables, ''), ',\
',
                                               '        "sort_rows": ', IFNULL(sort_rows, ''), ',\
',
                                               '        "sort_merge_passes": ', IFNULL(sort_merge_passes, ''), '\
',
                                               '      }'), '') ORDER BY event_id) AS stmts
                               FROM performance_schema.events_statements_history
                              WHERE sql_text IS NOT NULL
                                AND thread_id = in_thread_id
                              GROUP BY thread_id, nesting_event_id
                            ) AS s
                    ON trxi.thread_id = s.thread_id
                   AND trxi.event_id = s.nesting_event_id
                 WHERE trxi.thread_id = in_thread_id
                 GROUP BY trxi.thread_id, trxi.event_id
                ) trxs
          GROUP BY thread_id
    );
    IF (@old_group_concat_max_len IS NOT NULL) THEN
        SET SESSION group_concat_max_len = @old_group_concat_max_len;
    END IF;
    RETURN v_output;
END
ps_trace_statement_digest
BEGIN
    DECLARE v_start_fresh BOOLEAN DEFAULT false;
    DECLARE v_auto_enable BOOLEAN DEFAULT false;
    DECLARE v_explain     BOOLEAN DEFAULT true;
    DECLARE v_this_thread_enabed ENUM('YES', 'NO');
    DECLARE v_runtime INT DEFAULT 0;
    DECLARE v_start INT DEFAULT 0;
    DECLARE v_found_stmts INT;
    SET @log_bin := @@sql_log_bin;
    SET sql_log_bin = 0;
    SELECT INSTRUMENTED INTO v_this_thread_enabed FROM performance_schema.threads WHERE PROCESSLIST_ID = CONNECTION_ID();
    CALL sys.ps_setup_disable_thread(CONNECTION_ID());
    DROP TEMPORARY TABLE IF EXISTS stmt_trace;
    CREATE TEMPORARY TABLE stmt_trace (
        thread_id BIGINT UNSIGNED,
        timer_start BIGINT UNSIGNED,
        event_id BIGINT UNSIGNED,
        sql_text longtext,
        timer_wait BIGINT UNSIGNED,
        lock_time BIGINT UNSIGNED,
        errors BIGINT UNSIGNED,
        mysql_errno INT,
        rows_sent BIGINT UNSIGNED,
        rows_affected BIGINT UNSIGNED,
        rows_examined BIGINT UNSIGNED,
        created_tmp_tables BIGINT UNSIGNED,
        created_tmp_disk_tables BIGINT UNSIGNED,
        no_index_used BIGINT UNSIGNED,
        PRIMARY KEY (thread_id, timer_start)
    );
    DROP TEMPORARY TABLE IF EXISTS stmt_stages;
    CREATE TEMPORARY TABLE stmt_stages (
       event_id BIGINT UNSIGNED,
       stmt_id BIGINT UNSIGNED,
       event_name VARCHAR(128),
       timer_wait BIGINT UNSIGNED,
       PRIMARY KEY (event_id)
    );
    SET v_start_fresh = in_start_fresh;
    IF v_start_fresh THEN
        TRUNCATE TABLE performance_schema.events_statements_history_long;
        TRUNCATE TABLE performance_schema.events_stages_history_long;
    END IF;
    SET v_auto_enable = in_auto_enable;
    IF v_auto_enable THEN
        CALL sys.ps_setup_save(0);
        UPDATE performance_schema.threads
           SET INSTRUMENTED = IF(PROCESSLIST_ID IS NOT NULL, 'YES', 'NO');
        UPDATE performance_schema.setup_consumers
           SET ENABLED = 'YES'
         WHERE NAME NOT LIKE '%\\_history'
               AND NAME NOT LIKE 'events_wait%'
               AND NAME NOT LIKE 'events_transactions%'
               AND NAME <> 'statements_digest';
        UPDATE performance_schema.setup_instruments
           SET ENABLED = 'YES',
               TIMED   = 'YES'
         WHERE NAME LIKE 'statement/%' OR NAME LIKE 'stage/%';
    END IF;
    WHILE v_runtime < in_runtime DO
        SELECT UNIX_TIMESTAMP() INTO v_start;
        INSERT IGNORE INTO stmt_trace
        SELECT thread_id, timer_start, event_id, sql_text, timer_wait, lock_time, errors, mysql_errno,
               rows_sent, rows_affected, rows_examined, created_tmp_tables, created_tmp_disk_tables, no_index_used
          FROM performance_schema.events_statements_history_long
        WHERE digest = in_digest;
        INSERT IGNORE INTO stmt_stages
        SELECT stages.event_id, stmt_trace.event_id,
               stages.event_name, stages.timer_wait
          FROM performance_schema.events_stages_history_long AS stages
          JOIN stmt_trace ON stages.nesting_event_id = stmt_trace.event_id;
        SELECT SLEEP(in_interval) INTO @sleep;
        SET v_runtime = v_runtime + (UNIX_TIMESTAMP() - v_start);
    END WHILE;
    SELECT "SUMMARY STATISTICS";
    SELECT COUNT(*) executions,
           format_pico_time(SUM(timer_wait)) AS exec_time,
           format_pico_time(SUM(lock_time)) AS lock_time,
           SUM(rows_sent) AS rows_sent,
           SUM(rows_affected) AS rows_affected,
           SUM(rows_examined) AS rows_examined,
           SUM(created_tmp_tables) AS tmp_tables,
           SUM(no_index_used) AS full_scans
      FROM stmt_trace;
    SELECT event_name,
           COUNT(*) as count,
           format_pico_time(SUM(timer_wait)) as latency
      FROM stmt_stages
     GROUP BY event_name
     ORDER BY SUM(timer_wait) DESC;
    SELECT "LONGEST RUNNING STATEMENT";
    SELECT thread_id,
           format_pico_time(timer_wait) AS exec_time,
           format_pico_time(lock_time) AS lock_time,
           rows_sent,
           rows_affected,
           rows_examined,
           created_tmp_tables AS tmp_tables,
           no_index_used AS full_scan
      FROM stmt_trace
     ORDER BY timer_wait DESC LIMIT 1;
    SELECT sql_text
      FROM stmt_trace
     ORDER BY timer_wait DESC LIMIT 1;
    SELECT sql_text, event_id INTO @sql, @sql_id
      FROM stmt_trace
    ORDER BY timer_wait DESC LIMIT 1;
    IF (@sql_id IS NOT NULL) THEN
        SELECT event_name,
               format_pico_time(timer_wait) as latency
          FROM stmt_stages
         WHERE stmt_id = @sql_id
         ORDER BY event_id;
    END IF;
    DROP TEMPORARY TABLE stmt_trace;
    DROP TEMPORARY TABLE stmt_stages;
    IF (@sql IS NOT NULL) THEN
        SET @stmt := CONCAT("EXPLAIN FORMAT=JSON ", @sql);
        BEGIN
            DECLARE CONTINUE HANDLER FOR 1064, 1146 SET v_explain = false;
            PREPARE explain_stmt FROM @stmt;
        END;
        IF (v_explain) THEN
            EXECUTE explain_stmt;
            DEALLOCATE PREPARE explain_stmt;
        END IF;
    END IF;
    IF v_auto_enable THEN
        CALL sys.ps_setup_reload_saved();
    END IF;
    IF (v_this_thread_enabed = 'YES') THEN
        CALL sys.ps_setup_enable_thread(CONNECTION_ID());
    END IF;
    SET sql_log_bin = @log_bin;
END
DELIVERY
BEGIN
        DECLARE d_no_o_id  INTEGER;
        DECLARE current_rowid   INTEGER;
        DECLARE d_d_id      INTEGER;
        DECLARE d_c_id      INTEGER;
        DECLARE d_ol_total  INTEGER;
        DECLARE deliv_data  VARCHAR(100);
        DECLARE loop_counter    INT;
        DECLARE `Constraint Violation` CONDITION FOR SQLSTATE '23000';
        DECLARE EXIT HANDLER FOR `Constraint Violation` ROLLBACK;
        SET loop_counter = 1;
        START TRANSACTION;
        WHILE loop_counter <= 10 DO
        SET d_d_id = loop_counter;
        SELECT no_o_id INTO d_no_o_id FROM new_order WHERE no_w_id = d_w_id AND no_d_id = d_d_id LIMIT 1;
        DELETE FROM new_order WHERE no_w_id = d_w_id AND no_d_id = d_d_id AND no_o_id = d_no_o_id;
        SELECT o_c_id INTO d_c_id FROM orders
        WHERE o_id = d_no_o_id AND o_d_id = d_d_id AND
        o_w_id = d_w_id;
        UPDATE orders SET o_carrier_id = d_o_carrier_id
        WHERE o_id = d_no_o_id AND o_d_id = d_d_id AND
        o_w_id = d_w_id;
        UPDATE order_line SET ol_delivery_d = timestamp
        WHERE ol_o_id = d_no_o_id AND ol_d_id = d_d_id AND
        ol_w_id = d_w_id;
        SELECT SUM(ol_amount) INTO d_ol_total
        FROM order_line
        WHERE ol_o_id = d_no_o_id AND ol_d_id = d_d_id
        AND ol_w_id = d_w_id;
        UPDATE customer SET c_balance = c_balance + d_ol_total
        WHERE c_id = d_c_id AND c_d_id = d_d_id AND
        c_w_id = d_w_id;
        SET deliv_data = CONCAT(d_d_id,' ',d_no_o_id,' ',timestamp);
        set loop_counter = loop_counter + 1;
        END WHILE;
        COMMIT;
        END
NEWORD
BEGIN
        DECLARE no_ol_supply_w_id  INTEGER;
        DECLARE no_ol_i_id    INTEGER;
        DECLARE no_ol_quantity     INTEGER;
        DECLARE no_o_all_local     INTEGER;
        DECLARE o_id       INTEGER;
        DECLARE no_i_name    VARCHAR(24);
        DECLARE no_i_price    DECIMAL(5,2);
        DECLARE no_i_data    VARCHAR(50);
        DECLARE no_s_quantity    DECIMAL(6);
        DECLARE no_ol_amount    DECIMAL(6,2);
        DECLARE no_s_dist_01    CHAR(24);
        DECLARE no_s_dist_02    CHAR(24);
        DECLARE no_s_dist_03    CHAR(24);
        DECLARE no_s_dist_04    CHAR(24);
        DECLARE no_s_dist_05    CHAR(24);
        DECLARE no_s_dist_06    CHAR(24);
        DECLARE no_s_dist_07    CHAR(24);
        DECLARE no_s_dist_08    CHAR(24);
        DECLARE no_s_dist_09    CHAR(24);
        DECLARE no_s_dist_10    CHAR(24);
        DECLARE no_ol_dist_info   CHAR(24);
        DECLARE no_s_data       VARCHAR(50);
        DECLARE x        INTEGER;
        DECLARE rbk           INTEGER;
        DECLARE loop_counter    INT;
        DECLARE `Constraint Violation` CONDITION FOR SQLSTATE '23000';
        DECLARE EXIT HANDLER FOR `Constraint Violation` ROLLBACK;
        DECLARE EXIT HANDLER FOR NOT FOUND ROLLBACK;
        SET no_o_all_local = 1;
        SELECT c_discount, c_last, c_credit, w_tax
        INTO no_c_discount, no_c_last, no_c_credit, no_w_tax
        FROM customer, warehouse
        WHERE warehouse.w_id = no_w_id AND customer.c_w_id = no_w_id AND
        customer.c_d_id = no_d_id AND customer.c_id = no_c_id;
        START TRANSACTION;
        SELECT d_next_o_id, d_tax INTO no_d_next_o_id, no_d_tax
        FROM district
        WHERE d_id = no_d_id AND d_w_id = no_w_id FOR UPDATE;
        UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = no_d_id AND d_w_id = no_w_id;
        SET o_id = no_d_next_o_id;
        SET rbk = FLOOR(1 + (RAND() * 99));
        SET loop_counter = 1;
        WHILE loop_counter <= no_o_ol_cnt DO
        IF ((loop_counter = no_o_ol_cnt) AND (rbk = 1))
        THEN
        SET no_ol_i_id = 100001;
        ELSE
        SET no_ol_i_id = FLOOR(1 + (RAND() * 100000));
        END IF;
        SET x = FLOOR(1 + (RAND() * 100));
        IF ( x > 1 )
        THEN
        SET no_ol_supply_w_id = no_w_id;
        ELSE
        SET no_ol_supply_w_id = no_w_id;
        SET no_o_all_local = 0;
        WHILE ((no_ol_supply_w_id = no_w_id) AND (no_max_w_id != 1)) DO
        SET no_ol_supply_w_id = FLOOR(1 + (RAND() * no_max_w_id));
        END WHILE;
        END IF;
        SET no_ol_quantity = FLOOR(1 + (RAND() * 10));
        SELECT i_price, i_name, i_data INTO no_i_price, no_i_name, no_i_data
        FROM item WHERE i_id = no_ol_i_id;
        SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
        INTO no_s_quantity, no_s_data, no_s_dist_01, no_s_dist_02, no_s_dist_03, no_s_dist_04, no_s_dist_05, no_s_dist_06, no_s_dist_07, no_s_dist_08, no_s_dist_09, no_s_dist_10
        FROM stock WHERE s_i_id = no_ol_i_id AND s_w_id = no_ol_supply_w_id;
        IF ( no_s_quantity > no_ol_quantity )
        THEN
        SET no_s_quantity = ( no_s_quantity - no_ol_quantity );
        ELSE
        SET no_s_quantity = ( no_s_quantity - no_ol_quantity + 91 );
        END IF;
        UPDATE stock SET s_quantity = no_s_quantity
        WHERE s_i_id = no_ol_i_id
        AND s_w_id = no_ol_supply_w_id;
        SET no_ol_amount = (  no_ol_quantity * no_i_price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) );
        CASE no_d_id
        WHEN 1 THEN
        SET no_ol_dist_info = no_s_dist_01;
        WHEN 2 THEN
        SET no_ol_dist_info = no_s_dist_02;
        WHEN 3 THEN
        SET no_ol_dist_info = no_s_dist_03;
        WHEN 4 THEN
        SET no_ol_dist_info = no_s_dist_04;
        WHEN 5 THEN
        SET no_ol_dist_info = no_s_dist_05;
        WHEN 6 THEN
        SET no_ol_dist_info = no_s_dist_06;
        WHEN 7 THEN
        SET no_ol_dist_info = no_s_dist_07;
        WHEN 8 THEN
        SET no_ol_dist_info = no_s_dist_08;
        WHEN 9 THEN
        SET no_ol_dist_info = no_s_dist_09;
        WHEN 10 THEN
        SET no_ol_dist_info = no_s_dist_10;
        END CASE;
        INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
        VALUES (o_id, no_d_id, no_w_id, loop_counter, no_ol_i_id, no_ol_supply_w_id, no_ol_quantity, no_ol_amount, no_ol_dist_info);
        set loop_counter = loop_counter + 1;
        END WHILE;
        INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (o_id, no_d_id, no_w_id, no_c_id, timestamp, no_o_ol_cnt, no_o_all_local);
        INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (o_id, no_d_id, no_w_id);
        COMMIT;
        END
OSTAT
BEGIN 
        DECLARE  os_ol_i_id   INTEGER;
        DECLARE  os_ol_supply_w_id INTEGER;
        DECLARE  os_ol_quantity INTEGER;
        DECLARE  os_ol_amount   INTEGER;
        DECLARE  os_ol_delivery_d   DATETIME;
        DECLARE done      INT DEFAULT 0;
        DECLARE namecnt     INTEGER;
        DECLARE i         INTEGER;
        DECLARE loop_counter  INT;
        DECLARE no_order_status VARCHAR(100);
        DECLARE os_ol_i_id_array VARCHAR(200);
        DECLARE os_ol_supply_w_id_array VARCHAR(200);
        DECLARE os_ol_quantity_array VARCHAR(200);
        DECLARE os_ol_amount_array VARCHAR(200);
        DECLARE os_ol_delivery_d_array VARCHAR(420);
        DECLARE `Constraint Violation` CONDITION FOR SQLSTATE '23000';
        DECLARE c_name CURSOR FOR
        SELECT c_balance, c_first, c_middle, c_id
        FROM customer
        WHERE c_last = os_c_last AND c_d_id = os_d_id AND c_w_id = os_w_id
        ORDER BY c_first;
        DECLARE c_line CURSOR FOR
        SELECT ol_i_id, ol_supply_w_id, ol_quantity,
        ol_amount, ol_delivery_d
        FROM order_line
        WHERE ol_o_id = os_o_id AND ol_d_id = os_d_id AND ol_w_id = os_w_id;
        DECLARE EXIT HANDLER FOR `Constraint Violation` ROLLBACK;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
        set no_order_status = '';
        set os_ol_i_id_array = 'CSV,';
        set os_ol_supply_w_id_array = 'CSV,';
        set os_ol_quantity_array = 'CSV,';
        set os_ol_amount_array = 'CSV,';
        set os_ol_delivery_d_array = 'CSV,';
        START TRANSACTION;
        IF ( byname = 1 )
        THEN
        SELECT count(c_id) INTO namecnt
        FROM customer
        WHERE c_last = os_c_last AND c_d_id = os_d_id AND c_w_id = os_w_id;
        IF ( MOD (namecnt, 2) = 1 )
        THEN
        SET namecnt = (namecnt + 1);
        END IF;
        OPEN c_name;
        SET loop_counter = 0;
        WHILE loop_counter <= (namecnt/2) DO
        FETCH c_name
        INTO os_c_balance, os_c_first, os_c_middle, os_c_id;
        set loop_counter = loop_counter + 1;
        END WHILE;
        close c_name;
        ELSE
        SELECT c_balance, c_first, c_middle, c_last
        INTO os_c_balance, os_c_first, os_c_middle, os_c_last
        FROM customer
        WHERE c_id = os_c_id AND c_d_id = os_d_id AND c_w_id = os_w_id;
        END IF;
        set done = 0;
        SELECT o_id, o_carrier_id, o_entry_d
        INTO os_o_id, os_o_carrier_id, os_entdate
        FROM
        (SELECT o_id, o_carrier_id, o_entry_d
        FROM orders where o_d_id = os_d_id AND o_w_id = os_w_id and o_c_id = os_c_id
        ORDER BY o_id DESC) AS sb LIMIT 1;
        IF done THEN
        set no_order_status = 'No orders for customer';
        END IF;
        set done = 0;
        set i = 0;
        OPEN c_line;
        REPEAT
        FETCH c_line INTO os_ol_i_id, os_ol_supply_w_id, os_ol_quantity, os_ol_amount, os_ol_delivery_d;
        IF NOT done THEN
        set os_ol_i_id_array = CONCAT(os_ol_i_id_array,',',CAST(i AS CHAR),',',CAST(os_ol_i_id AS CHAR));
        set os_ol_supply_w_id_array = CONCAT(os_ol_supply_w_id_array,',',CAST(i AS CHAR),',',CAST(os_ol_supply_w_id AS CHAR));
        set os_ol_quantity_array = CONCAT(os_ol_quantity_array,',',CAST(i AS CHAR),',',CAST(os_ol_quantity AS CHAR));
        set os_ol_amount_array = CONCAT(os_ol_amount_array,',',CAST(i AS CHAR),',',CAST(os_ol_amount AS CHAR));
        set os_ol_delivery_d_array = CONCAT(os_ol_delivery_d_array,',',CAST(i AS CHAR),',',CAST(os_ol_delivery_d AS CHAR));
        set i = i+1;
        END IF;
        UNTIL done END REPEAT;
        CLOSE c_line;
        COMMIT;
        END
PAYMENT
BEGIN
        DECLARE done      INT DEFAULT 0;
        DECLARE  namecnt    INTEGER;
        DECLARE p_d_name  VARCHAR(11);
        DECLARE p_w_name  VARCHAR(11);
        DECLARE p_c_new_data  VARCHAR(500);
        DECLARE h_data    VARCHAR(30);
        DECLARE loop_counter    INT;
        DECLARE `Constraint Violation` CONDITION FOR SQLSTATE '23000';
        DECLARE c_byname CURSOR FOR
        SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
        FROM customer
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_last = p_c_last
        ORDER BY c_first;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
        DECLARE EXIT HANDLER FOR `Constraint Violation` ROLLBACK;
        START TRANSACTION;
        UPDATE warehouse SET w_ytd = w_ytd + p_h_amount
        WHERE w_id = p_w_id;
        SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
        INTO p_w_street_1, p_w_street_2, p_w_city, p_w_state, p_w_zip, p_w_name
        FROM warehouse
        WHERE w_id = p_w_id;
        UPDATE district SET d_ytd = d_ytd + p_h_amount
        WHERE d_w_id = p_w_id AND d_id = p_d_id;
        SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
        INTO p_d_street_1, p_d_street_2, p_d_city, p_d_state, p_d_zip, p_d_name
        FROM district
        WHERE d_w_id = p_w_id AND d_id = p_d_id;
        IF (byname = 1)
        THEN
        SELECT count(c_id) INTO namecnt
        FROM customer
        WHERE c_last = p_c_last AND c_d_id = p_c_d_id AND c_w_id = p_c_w_id;
        OPEN c_byname;
        IF ( MOD (namecnt, 2) = 1 )
        THEN
        SET namecnt = (namecnt + 1);
        END IF;
        SET loop_counter = 0;
        WHILE loop_counter <= (namecnt/2) DO
        FETCH c_byname
        INTO p_c_first, p_c_middle, p_c_id, p_c_street_1, p_c_street_2, p_c_city,
        p_c_state, p_c_zip, p_c_phone, p_c_credit, p_c_credit_lim, p_c_discount, p_c_balance, p_c_since;
        set loop_counter = loop_counter + 1;
        END WHILE;
        CLOSE c_byname;
        ELSE
        SELECT c_first, c_middle, c_last,
        c_street_1, c_street_2, c_city, c_state, c_zip,
        c_phone, c_credit, c_credit_lim,
        c_discount, c_balance, c_since
        INTO p_c_first, p_c_middle, p_c_last,
        p_c_street_1, p_c_street_2, p_c_city, p_c_state, p_c_zip,
        p_c_phone, p_c_credit, p_c_credit_lim,
        p_c_discount, p_c_balance, p_c_since
        FROM customer
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = p_c_id;
        END IF;
        SET p_c_balance = ( p_c_balance + p_h_amount );
        IF p_c_credit = 'BC'
        THEN
        SELECT c_data INTO p_c_data
        FROM customer
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = p_c_id;
        SET h_data = CONCAT(p_w_name,' ',p_d_name);
        SET p_c_new_data = CONCAT(CAST(p_c_id AS CHAR),' ',CAST(p_c_d_id AS CHAR),' ',CAST(p_c_w_id AS CHAR),' ',CAST(p_d_id AS CHAR),' ',CAST(p_w_id AS CHAR),' ',CAST(FORMAT(p_h_amount,2) AS CHAR),CAST(timestamp AS CHAR),h_data);
        SET p_c_new_data = SUBSTR(CONCAT(p_c_new_data,p_c_data),1,500-(LENGTH(p_c_new_data)));
        UPDATE customer
        SET c_balance = p_c_balance, c_data = p_c_new_data
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND
        c_id = p_c_id;
        ELSE
        UPDATE customer SET c_balance = p_c_balance
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND
        c_id = p_c_id;
        END IF;
        SET h_data = CONCAT(p_w_name,' ',p_d_name);
        INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
        VALUES (p_c_d_id, p_c_w_id, p_c_id, p_d_id, p_w_id, timestamp, p_h_amount, h_data);
        COMMIT;
        END
