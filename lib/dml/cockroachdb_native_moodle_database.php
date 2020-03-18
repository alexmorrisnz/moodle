<?php
// This file is part of Moodle - http://moodle.org/
//
// Moodle is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Moodle is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Moodle.  If not, see <http://www.gnu.org/licenses/>.

/**
 * Native cockroachdb class representing moodle database interface.
 *
 * @package    core_dml
 * @copyright  2020 Jamie Chapman-Brown
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */

defined('MOODLE_INTERNAL') || die();

require_once(__DIR__.'/moodle_database.php');
require_once(__DIR__.'/pgsql_native_moodle_recordset.php');
require_once(__DIR__.'/moodle_temptables.php');
require_once(__DIR__.'/pgsql_native_moodle_database.php');

/**
 * Native pgsql class representing moodle database interface.
 *
 * @package    core_dml
 * @copyright  2020 Jamie Chapman-Brown
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */
class cockroachdb_native_moodle_database extends pgsql_native_moodle_database {
    /**
     * Do NOT use in code, to be used by database_manager only!
     * @param string|array $sql query
     * @param array|null $tablenames an array of xmldb table names affected by this request.
     * @return bool true
     * @throws ddl_change_structure_exception A DDL specific exception is thrown for any errors.
     */
    public function change_database_structure($sql, $tablenames = null) {
        $this->get_manager(); // Includes DDL exceptions classes ;-)

        #error_log(print_r($sql, true));
        if (is_array($sql)) {
            $sql = implode("\n;\n", $sql) . ';';
        }

        #error_log($sql);
        $this->query_start($sql, null, SQL_QUERY_STRUCTURE);
        $result = pg_query($this->pgsql, $sql);
        $this->query_end($result);
        pg_free_result($result);

        $this->reset_caches($tablenames);
        return true;
    }

    public function connect($dbhost, $dbuser, $dbpass, $dbname, $prefix, array $dboptions=null) {
        global $DB;

        # cockroach db default is 26257 rather than 5432
        if (empty($dboptions['dbport'])) {
            $dboptions['dbport'] = '26257';
        }

        # fetchbuffer must be set to 0 to disable cursors
        # cockroachdb does not support cursors: https://github.com/cockroachdb/cockroach/issues/30352
        $dboptions['fetchbuffersize'] = "0";

        parent::connect($dbhost, $dbuser, $dbpass, $dbname, $prefix, $dboptions);

        # turn on expiremental cockroach savepoints
        $this->execute('SET force_savepoint_restart=true');

        # set expiremental serial normalization
        $this->execute('SET experimental_serial_normalization TO sql_sequence');

        $this->temptables = new moodle_temptables($this);
    }

    /**
     * Called immediately after each db query.
     * @param mixed db specific result
     * @return void
     */
    protected function query_end($result) {
        // reset original debug level
        error_reporting($this->last_error_reporting);
        
        parent::query_end($result);
    }

    /**
     * Driver specific start of real database transaction,
     * this can not be used directly in code.
     * @return void
     */
    protected function begin_transaction() {
        $sql = "BEGIN ISOLATION LEVEL READ COMMITTED;";
        $this->query_start($sql, null, SQL_QUERY_AUX);
        $result = pg_query($this->pgsql, $sql);
        $this->query_end($result);

        pg_free_result($result);
    }

    /**
     * Driver specific commit of real database transaction,
     * this can not be used directly in code.
     * @return void
     */
    protected function commit_transaction() {
        $sql = "COMMIT";
        $this->query_start($sql, null, SQL_QUERY_AUX);
        $result = pg_query($this->pgsql, $sql);
        $this->query_end($result);

        pg_free_result($result);
    }

    
    public function sql_concat() {
        $arr = func_get_args();
        # cast explicitly for concat, cockroachdb does not allow
        # implicit casts on sql concats
        # https://github.com/cockroachdb/cockroach/issues/34404
        $func = function($value) {
            if (preg_match("/\'.*\'/", $value, $match)){
                return $value;
            }
            else {
                return $value .'::text';
            }
        };

        $arg = array_map($func, $arr);
        $s = implode(' || ', $arg);

        if ($s === '') {
            return " '' ";
        }
        // Add always empty string element so integer-exclusive concats
        // will work without needing to cast each element explicitly
        return " '' || $s ";
    }


    public function sql_concat_join($separator="' '", $elements=array()) {
        for ($n=count($elements)-1; $n > 0 ; $n--) {
            array_splice($elements, $n, 0, $separator);
        }
        $func = function($value) {
            if (preg_match("/\'.*\'/", $value, $match)){
                return $value;
            }
            else {
                return $value .'::text';
            }
        };
    
    
        $elements = array_map($func, $elements);


        $s = implode(' || ', $elements);
        if ($s === '') {
            return " '' ";
        }
        return " $s ";
    }

    /**
     * Sql to cast a string to an integer
     * cast to decimal frist because
     * moodle and its tests will always use base10 strings
     * and cockroach will cast using notations which fail in tests
     */
    public function sql_cast_char2int($fieldname, $text=false) {
        return $fieldname . '::decimal::int ';
    }  

    /**
     * Returns detailed information about columns in table.
     *
     * @param string $table name
     * @return database_column_info[] array of database_column_info objects indexed with column names
     */
    protected function fetch_columns(string $table): array {

        $structure = array();

        $tablename = $this->prefix.$table;

        $sql = 'SHOW COLUMNS FROM ' . $tablename;

        try {
            $this->query_start($sql, null, SQL_QUERY_AUX);
            $result = pg_query($this->pgsql, $sql);
            $this->query_end($result);
        }
        # todo: make this nicer, ie catch a smaller exception or do a query that returns empty
        catch (moodle_exception $e) {
            return array();
        }

        while ($rawcolumn = pg_fetch_object($result)) {

            $info = new stdClass();
            $info->name = $rawcolumn->column_name;
            $matches = null;

            if (preg_match('/VARCHAR\((\d+)\)/i', $rawcolumn->data_type, $matches)) {
                $info->type          = 'varchar';
                $info->meta_type     = 'C';
                $info->max_length    = $matches[1];
                $info->scale         = null;
                $info->not_null      = ($rawcolumn->is_nullable == 'f');
                $info->has_default   = ($rawcolumn->column_default != '');
                if ($info->has_default) {
                    $parts = explode('::', $rawcolumn->column_default);
                    if (count($parts) > 1) {
                        $info->default_value = reset($parts);
                        $info->default_value = trim($info->default_value, "'");
                    } else {
                        $info->default_value = $rawcolumn->column_default;
                    }
                } else {
                    $info->default_value = null;
                }
                $info->primary_key   = false;
                $info->binary        = false;
                $info->unsigned      = null;
                $info->auto_increment= false;
                $info->unique        = null;

            } else if (preg_match('/INT(\d)/i', $rawcolumn->data_type, $matches)) {
                $info->type = 'int';
                if (strpos($rawcolumn->column_default, 'nextval') === 0) {
                    $info->primary_key   = true;
                    $info->meta_type     = 'R';
                    $info->unique        = true;
                    $info->auto_increment= true;
                    $info->has_default   = false;
                } else {
                    $info->primary_key   = false;
                    $info->meta_type     = 'I';
                    $info->unique        = null;
                    $info->auto_increment= false;
                    $info->has_default   = ($rawcolumn->column_default != '');
                }
                // Return number of decimals, not bytes here.
                if ($matches[1] >= 8) {
                    $info->max_length = 18;
                } else if ($matches[1] >= 4) {
                    $info->max_length = 9;
                } else if ($matches[1] >= 2) {
                    $info->max_length = 4;
                } else if ($matches[1] >= 1) {
                    $info->max_length = 2;
                } else {
                    $info->max_length = 0;
                }
                $info->scale         = null;
                $info->not_null      = ($rawcolumn->is_nullable == 'f');
                if ($info->has_default) {
                    // PG 9.5+ uses ::<TYPE> syntax for some defaults.
                    $parts = explode('::', $rawcolumn->column_default);
                    if (count($parts) > 1) {
                        $info->default_value = reset($parts);
                    } else {
                        $info->default_value = $rawcolumn->column_default;
                    }
                    $info->default_value = trim($info->default_value, "()'");
                } else {
                    $info->default_value = null;
                }
                $info->binary        = false;
                $info->unsigned      = false;

            } else if (preg_match('/DECIMAL\((\d+)(,(\d+))?\)/i', $rawcolumn->data_type, $matches)) {
                $info->type = 'numeric';
                $info->meta_type     = 'N';
                $info->primary_key   = false;
                $info->binary        = false;
                $info->unsigned      = null;
                $info->auto_increment= false;
                $info->unique        = null;
                $info->not_null      = ($rawcolumn->is_nullable == 'f');
                $info->has_default   = ($rawcolumn->column_default != '');
                if ($info->has_default) {
                    // PG 9.5+ uses ::<TYPE> syntax for some defaults.
                    $parts = explode('::', $rawcolumn->column_default);
                    if (count($parts) > 1) {
                        $info->default_value = reset($parts);
                    } else {
                        $info->default_value = $rawcolumn->column_default;
                    }
                    $info->default_value = trim($info->default_value, "()'");
                } else {
                    $info->default_value = null;
                }
                
                $info->max_length = $matches[1];
                
                if (count($matches) == 4 ){
                    $info->scale = $matches[3];
                }
                else {
                    $info->scale = 0;
                }

            } else if (preg_match('/FLOAT(\d)/i', $rawcolumn->data_type, $matches)) {
                $info->type = 'float';
                $info->meta_type     = 'N';
                $info->primary_key   = false;
                $info->binary        = false;
                $info->unsigned      = null;
                $info->auto_increment= false;
                $info->unique        = null;
                $info->not_null      = ($rawcolumn->is_nullable == 'f');
                $info->has_default   = ($rawcolumn->column_default != '');
                if ($info->has_default) {
                    // PG 9.5+ uses ::<TYPE> syntax for some defaults.
                    $parts = explode('::', $rawcolumn->column_default);
                    if (count($parts) > 1) {
                        $info->default_value = reset($parts);
                    } else {
                        $info->default_value = $rawcolumn->column_default;
                    }
                    $info->default_value = trim($info->default_value, "()'");
                } else {
                    $info->default_value = null;
                }
                // just guess expected number of decimal places :-(
                if ($matches[1] == 8) {
                    // total 15 digits
                    $info->max_length = 8;
                    $info->scale      = 7;
                } else {
                    // total 6 digits
                    $info->max_length = 4;
                    $info->scale      = 2;
                }

            } else if ($rawcolumn->data_type === 'STRING') {
                $info->type          = 'text';
                $info->meta_type     = 'X';
                $info->max_length    = -1;
                $info->scale         = null;
                $info->not_null      = ($rawcolumn->is_nullable == 'f');
                $info->has_default   = ($rawcolumn->column_default != '');
                if ($info->has_default) {
                    $parts = explode('::', $rawcolumn->column_default);
                    if (count($parts) > 1) {
                        $info->default_value = reset($parts);
                        $info->default_value = trim($info->default_value, "'");
                    } else {
                        $info->default_value = $rawcolumn->column_default;
                    }
                } else {
                    $info->default_value = null;
                }
                $info->primary_key   = false;
                $info->binary        = false;
                $info->unsigned      = null;
                $info->auto_increment= false;
                $info->unique        = null;

            } else if ($rawcolumn->data_type === 'BYTES') {
                $info->type          = 'bytea';
                $info->meta_type     = 'B';
                $info->max_length    = -1;
                $info->scale         = null;
                $info->not_null      = ($rawcolumn->is_nullable == 'f');
                $info->has_default   = false;
                $info->default_value = null;
                $info->primary_key   = false;
                $info->binary        = true;
                $info->unsigned      = null;
                $info->auto_increment= false;
                $info->unique        = null;

            }

            $structure[$info->name] = new database_column_info($info);
        }

        pg_free_result($result);

        return $structure;
    }

    /**
     * Return table indexes - everything lowercased.
     * cockroachdb seems to add databasename to the indexdef
     *
     * @param string $table The table we want to get indexes from.
     * @return array of arrays
     */
    public function get_indexes($table) {
        $indexes = array();
        $tablename = $this->prefix.$table;

        
        $sql = "SELECT i.*
                  FROM pg_catalog.pg_indexes i
                  JOIN pg_catalog.pg_namespace as ns ON ns.nspname = i.schemaname
                 WHERE i.tablename = '$tablename'
                       AND (i.schemaname = current_schema() OR ns.oid = pg_my_temp_schema())";

        $this->query_start($sql, null, SQL_QUERY_AUX);
        $result = pg_query($this->pgsql, $sql);
        $this->query_end($result);

        
        if ($result) {
            while ($row = pg_fetch_assoc($result)) {
                // The index definition could be generated schema-qualifying the target table name
                // for safety, depending on the pgsql version (CVE-2018-1058).
                $regex = '/CREATE (|UNIQUE )INDEX ([^\s]+) ON '. $this->dbname . '\.(|'.$row['schemaname'].'\.)'.$tablename.' USING ([^\s]+) \(([^\)]+)\)/i';
                if (!preg_match( $regex, $row['indexdef'], $matches)) {
                    continue;
                }
                if ($matches[5] === 'id ASC') {
                    continue;
                }
                $columns = explode(',', $matches[5]);
                foreach ($columns as $k=>$column) {
                    $column = trim($column);
                    if ($pos = strpos($column, ' ')) {
                        // index type is separated by space
                        $column = substr($column, 0, $pos);
                    }
                    $columns[$k] = $this->trim_quotes($column);
                }
                $indexes[$row['indexname']] = array('unique'=>!empty($matches[1]),
                                              'columns'=>$columns);
            }
            pg_free_result($result);
        }
        return $indexes;
    }

    /**
     * Cockroachdb seems to require explicit cast to int for substring args
     *
     * @param string $expr Some string field, no aggregates.
     * @param mixed $start Integer or expression evaluating to integer (1 based value; first char has index 1)
     * @param mixed $length Optional integer or expression evaluating to integer.
     * @return string The sql substring extraction fragment.
     */
    public function sql_substr($expr, $start, $length=false) {
        if (count(func_get_args()) < 2) {
            throw new coding_exception('moodle_database::sql_substr() requires at least two parameters', 'Originally this function was only returning name of SQL substring function, it now requires all parameters.');
        }
        if ($length === false) {
            return "SUBSTR($expr, $start::int)";
        } else {
            return "SUBSTR($expr, $start::int, $length::int)";
        }
    }


    /**
     * Helper function trimming (whitespace + quotes) any string
     * needed because PG uses to enclose with double quotes some
     * fields in indexes definition and others
     *
     * @param string $str string to apply whitespace + quotes trim
     * @return string trimmed string
     */
    private function trim_quotes($str) {
        return trim(trim($str), "'\"");
    }

    /**
     * Returns database family type - describes SQL dialect
     * Note: can be used before connect()
     * @return string db family name (mysql, postgres, mssql, oracle, etc.)
     */
    public function get_dbfamily() {
        return 'cockroachdb';
    }

    /**
     * Returns more specific database driver type
     * Note: can be used before connect()
     * @return string db type mysqli, pgsql, oci, mssql, sqlsrv
     */
    protected function get_dbtype() {
        return 'cockroachdb';
    }

    public function get_dbvendor() {
        return parent::get_dbfamily();
    }

}