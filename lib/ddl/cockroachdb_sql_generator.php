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
 * PostgreSQL specific SQL code generator.
 *
 * @package    core_ddl
 * @copyright  1999 onwards Martin Dougiamas     http://dougiamas.com
 *             2001-3001 Eloy Lafuente (stronk7) http://contiento.com
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */

defined('MOODLE_INTERNAL') || die();

require_once($CFG->libdir.'/ddl/sql_generator.php');
require_once($CFG->libdir.'/ddl/postgres_sql_generator.php');

/**
 * This class generate SQL code to be used against PostgreSQL
 * It extends XMLDBgenerator so everything can be
 * overridden as needed to generate correct SQL.
 *
 * @package    core_ddl
 * @copyright  1999 onwards Martin Dougiamas     http://dougiamas.com
 *             2001-3001 Eloy Lafuente (stronk7) http://contiento.com
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */

class cockroachdb_sql_generator extends postgres_sql_generator {
    public function getResetSequenceSQL($table) {

        if ($table instanceof xmldb_table) {
            $tablename = $table->getName();
        } else {
            $tablename = $table;
        }

        // From http://www.postgresql.org/docs/7.4/static/sql-altersequence.html
        $value = (int)$this->mdb->get_field_sql('SELECT MAX(id) FROM {'.$tablename.'}');
        $value++;
        return array("SELECT setval( '$this->prefix{$tablename}_id_seq', $value, false)");
    }

    /**
     * Returns detailed information about columns in table. This information is cached internally.
     * @param string $table name
     * @param bool $usecache
     * @return database_column_info[] array of database_column_info objects indexed with column names
     */
    public function get_columns($table, $usecache=true) {
        if ($usecache) {
            if ($this->temptables->is_temptable($table)) {
                if ($data = $this->get_temp_tables_cache()->get($table)) {
                    return $data;
                }
            } else {
                if ($data = $this->get_metacache()->get($table)) {
                    return $data;
                }
            }
        }

        $structure = array();

        $tablename = $this->prefix.$table;

        $sql = "SELECT a.attnum, a.attname AS field, t.typname AS type, a.attlen, a.atttypmod, a.attnotnull, a.atthasdef, d.adsrc
                  FROM pg_catalog.pg_class c
                  JOIN pg_catalog.pg_namespace as ns ON ns.oid = c.relnamespace
                  JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                  JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
             LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid = c.oid AND d.adnum = a.attnum)
                 WHERE relkind = 'r' AND c.relname = '$tablename' AND a.attnum > 0
                       AND (ns.nspname = current_schema() OR ns.oid = pg_my_temp_schema())
              ORDER BY a.attnum";

        $this->query_start($sql, null, SQL_QUERY_AUX);
        $result = pg_query($this->pgsql, $sql);
        $this->query_end($result);

        if (!$result) {
            return array();
        }
        while ($rawcolumn = pg_fetch_object($result)) {

            $info = new stdClass();
            $info->name = $rawcolumn->field;
            $matches = null;

            if ($rawcolumn->type === 'varchar') {
                $info->type          = 'varchar';
                $info->meta_type     = 'C';
                $info->max_length    = $rawcolumn->atttypmod - 4;
                $info->scale         = null;
                $info->not_null      = ($rawcolumn->attnotnull === 't');
                $info->has_default   = ($rawcolumn->atthasdef === 't');
                if ($info->has_default) {
                    $parts = explode('::', $rawcolumn->adsrc);
                    if (count($parts) > 1) {
                        $info->default_value = reset($parts);
                        $info->default_value = trim($info->default_value, "'");
                    } else {
                        $info->default_value = $rawcolumn->adsrc;
                    }
                } else {
                    $info->default_value = null;
                }
                $info->primary_key   = false;
                $info->binary        = false;
                $info->unsigned      = null;
                $info->auto_increment= false;
                $info->unique        = null;

            } else if (preg_match('/int(\d)/i', $rawcolumn->type, $matches)) {
                $info->type = 'int';
                if (strpos($rawcolumn->adsrc, 'nextval') === 0) {
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
                    $info->has_default   = ($rawcolumn->atthasdef === 't');
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
                $info->not_null      = ($rawcolumn->attnotnull === 't');
                if ($info->has_default) {
                    // PG 9.5+ uses ::<TYPE> syntax for some defaults.
                    $parts = explode('::', $rawcolumn->adsrc);
                    if (count($parts) > 1) {
                        $info->default_value = reset($parts);
                    } else {
                        $info->default_value = $rawcolumn->adsrc;
                    }
                    $info->default_value = trim($info->default_value, "()'");
                } else {
                    $info->default_value = null;
                }
                $info->binary        = false;
                $info->unsigned      = false;

            } else if ($rawcolumn->type === 'numeric') {
                $info->type = $rawcolumn->type;
                $info->meta_type     = 'N';
                $info->primary_key   = false;
                $info->binary        = false;
                $info->unsigned      = null;
                $info->auto_increment= false;
                $info->unique        = null;
                $info->not_null      = ($rawcolumn->attnotnull === 't');
                $info->has_default   = ($rawcolumn->atthasdef === 't');
                if ($info->has_default) {
                    // PG 9.5+ uses ::<TYPE> syntax for some defaults.
                    $parts = explode('::', $rawcolumn->adsrc);
                    if (count($parts) > 1) {
                        $info->default_value = reset($parts);
                    } else {
                        $info->default_value = $rawcolumn->adsrc;
                    }
                    $info->default_value = trim($info->default_value, "()'");
                } else {
                    $info->default_value = null;
                }
                $info->max_length    = $rawcolumn->atttypmod >> 16;
                $info->scale         = ($rawcolumn->atttypmod & 0xFFFF) - 4;

            } else if (preg_match('/float(\d)/i', $rawcolumn->type, $matches)) {
                $info->type = 'float';
                $info->meta_type     = 'N';
                $info->primary_key   = false;
                $info->binary        = false;
                $info->unsigned      = null;
                $info->auto_increment= false;
                $info->unique        = null;
                $info->not_null      = ($rawcolumn->attnotnull === 't');
                $info->has_default   = ($rawcolumn->atthasdef === 't');
                if ($info->has_default) {
                    // PG 9.5+ uses ::<TYPE> syntax for some defaults.
                    $parts = explode('::', $rawcolumn->adsrc);
                    if (count($parts) > 1) {
                        $info->default_value = reset($parts);
                    } else {
                        $info->default_value = $rawcolumn->adsrc;
                    }
                    $info->default_value = trim($info->default_value, "()'");
                } else {
                    $info->default_value = null;
                }
                // just guess expected number of deciaml places :-(
                if ($matches[1] == 8) {
                    // total 15 digits
                    $info->max_length = 8;
                    $info->scale      = 7;
                } else {
                    // total 6 digits
                    $info->max_length = 4;
                    $info->scale      = 2;
                }

            } else if ($rawcolumn->type === 'text') {
                $info->type          = $rawcolumn->type;
                $info->meta_type     = 'X';
                $info->max_length    = -1;
                $info->scale         = null;
                $info->not_null      = ($rawcolumn->attnotnull === 't');
                $info->has_default   = ($rawcolumn->atthasdef === 't');
                if ($info->has_default) {
                    $parts = explode('::', $rawcolumn->adsrc);
                    if (count($parts) > 1) {
                        $info->default_value = reset($parts);
                        $info->default_value = trim($info->default_value, "'");
                    } else {
                        $info->default_value = $rawcolumn->adsrc;
                    }
                } else {
                    $info->default_value = null;
                }
                $info->primary_key   = false;
                $info->binary        = false;
                $info->unsigned      = null;
                $info->auto_increment= false;
                $info->unique        = null;

            } else if ($rawcolumn->type === 'bytea') {
                $info->type          = $rawcolumn->type;
                $info->meta_type     = 'B';
                $info->max_length    = -1;
                $info->scale         = null;
                $info->not_null      = ($rawcolumn->attnotnull === 't');
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

        if ($usecache) {
            if ($this->temptables->is_temptable($table)) {
                $this->get_temp_tables_cache()->set($table, $structure);
            } else {
                $this->get_metacache()->set($table, $structure);
            }
        }

        return $structure;
    }

        /**
     * Returns the code (array of statements) needed to execute extra statements on table rename.
     *
     * @param xmldb_table $xmldb_table The xmldb_table object instance.
     * @param string $newname The new name for the table.
     * @return array Array of extra SQL statements to rename a table.
     */
    public function getRenameTableExtraSQL($xmldb_table, $newname) {

        $results = array();

        $newt = new xmldb_table($newname);

        $xmldb_field = new xmldb_field('id'); // Fields having sequences should be exclusively, id.

        $oldseqname = $this->getTableName($xmldb_table) . '_' . $xmldb_field->getName() . '_seq';
        $newseqname = $this->getTableName($newt) . '_' . $xmldb_field->getName() . '_seq';

        # remove the defualt from the column otherwise we can't rename the sequence as per
        # https://www.cockroachlabs.com/docs/stable/rename-sequence.html
        $results[] = 'ALTER TABLE ' . $this->getTableName($newt) . " ALTER COLUMN " . $xmldb_field->getName() . ' DROP DEFAULT';

        // Rename the sequence
        $results[] = 'ALTER SEQUENCE ' . $oldseqname . ' RENAME TO ' . $newseqname;
        
        # re-add the default sequence
        $results[] = 'ALTER TABLE ' . $this->getTableName($newt) . " ALTER COLUMN " . $xmldb_field->getName() . ' SET DEFAULT ' . "nextval('" . $newseqname . "':::STRING)";

        return $results;
    }

    /**
     * Given one xmldb_table and one xmldb_field, return the SQL statements needed to alter the field in the table.
     *
     * PostgreSQL has some severe limits:
     *     - Any change of type or precision requires a new temporary column to be created, values to
     *       be transfered potentially casting them, to apply defaults if the column is not null and
     *       finally, to rename it
     *     - Changes in null/not null require the SET/DROP NOT NULL clause
     *     - Changes in default require the SET/DROP DEFAULT clause
     *
     * @param xmldb_table $xmldb_table The table related to $xmldb_field.
     * @param xmldb_field $xmldb_field The instance of xmldb_field to create the SQL from.
     * @param string $skip_type_clause The type clause on alter columns, NULL by default.
     * @param string $skip_default_clause The default clause on alter columns, NULL by default.
     * @param string $skip_notnull_clause The null/notnull clause on alter columns, NULL by default.
     * @return string The field altering SQL statement.
     */
    public function getAlterFieldSQL($xmldb_table, $xmldb_field, $skip_type_clause = NULL, $skip_default_clause = NULL, $skip_notnull_clause = NULL) {

        $results = array();     // To store all the needed SQL commands

        // Get the normal names of the table and field
        $tablename = $xmldb_table->getName();
        $fieldname = $xmldb_field->getName();

        // Take a look to field metadata
        $meta = $this->mdb->get_columns($tablename);
        $metac = $meta[$xmldb_field->getName()];
        $oldmetatype = $metac->meta_type;
        $oldlength = $metac->max_length;
        $olddecimals = empty($metac->scale) ? null : $metac->scale;
        $oldnotnull = empty($metac->not_null) ? false : $metac->not_null;
        $olddefault = empty($metac->has_default) ? null : $metac->default_value;

        $typechanged = true;  //By default, assume that the column type has changed
        $precisionchanged = true;  //By default, assume that the column precision has changed
        $decimalchanged = true;  //By default, assume that the column decimal has changed
        $defaultchanged = true;  //By default, assume that the column default has changed
        $notnullchanged = true;  //By default, assume that the column notnull has changed

        // Detect if we are changing the type of the column
        if (($xmldb_field->getType() == XMLDB_TYPE_INTEGER && $oldmetatype == 'I') ||
            ($xmldb_field->getType() == XMLDB_TYPE_NUMBER  && $oldmetatype == 'N') ||
            ($xmldb_field->getType() == XMLDB_TYPE_FLOAT   && $oldmetatype == 'F') ||
            ($xmldb_field->getType() == XMLDB_TYPE_CHAR    && $oldmetatype == 'C') ||
            ($xmldb_field->getType() == XMLDB_TYPE_TEXT    && $oldmetatype == 'X') ||
            ($xmldb_field->getType() == XMLDB_TYPE_BINARY  && $oldmetatype == 'B')) {
            $typechanged = false;
        }
        // Detect if we are changing the precision
        if (($xmldb_field->getType() == XMLDB_TYPE_TEXT) ||
            ($xmldb_field->getType() == XMLDB_TYPE_BINARY) ||
            ($oldlength == -1) ||
            ($xmldb_field->getLength() == $oldlength)) {
            $precisionchanged = false;
        }
        // Detect if we are changing the decimals
        if (($xmldb_field->getType() == XMLDB_TYPE_INTEGER) ||
            ($xmldb_field->getType() == XMLDB_TYPE_CHAR) ||
            ($xmldb_field->getType() == XMLDB_TYPE_TEXT) ||
            ($xmldb_field->getType() == XMLDB_TYPE_BINARY) ||
            (!$xmldb_field->getDecimals()) ||
            (!$olddecimals) ||
            ($xmldb_field->getDecimals() == $olddecimals)) {
            $decimalchanged = false;
        }
        // Detect if we are changing the default
        if (($xmldb_field->getDefault() === null && $olddefault === null) ||
            ($xmldb_field->getDefault() === $olddefault)) {
            $defaultchanged = false;
        }
        // Detect if we are changing the nullability
        if (($xmldb_field->getNotnull() === $oldnotnull)) {
            $notnullchanged = false;
        }

        // Get the quoted name of the table and field
        $tablename = $this->getTableName($xmldb_table);
        $fieldname = $this->getEncQuoted($xmldb_field->getName());

        // Decide if we have changed the column specs (type/precision/decimals)
        $specschanged = $typechanged || $precisionchanged || $decimalchanged;

        # add some randomness into the columnname to mitigate leftover temp columns
        # as there is some possiblity of these being leftover if moodle is 
        # killed during a migration
        $tempcolumnnname = $fieldname . '__temp' . rand();

        // if specs have changed, need to alter column
        if ($specschanged) {

            $results[] = 'ALTER TABLE '. $tablename . ' ADD COLUMN ' . $tempcolumnnname . $this->getFieldSQL($xmldb_table, $xmldb_field, null, true, true, null, false);

            $alterstmt = 'UPDATE '. $tablename . ' SET ' . $tempcolumnnname . ' = ';

            #$alterstmt = 'ALTER TABLE ' . $tablename . ' ALTER COLUMN ' . $this->getEncQuoted($xmldb_field->getName()) .
            #    ' TYPE' . $this->getFieldSQL($xmldb_table, $xmldb_field, null, true, true, null, false);
            // Some castings must be performed explicitly (mainly from text|char to numeric|integer)
            if (($oldmetatype == 'C' || $oldmetatype == 'X') &&
                ($xmldb_field->getType() == XMLDB_TYPE_NUMBER || $xmldb_field->getType() == XMLDB_TYPE_FLOAT)) {
                $alterstmt .= ' CAST('.$fieldname.' AS NUMERIC)'; // from char or text to number or float
            } else if (($oldmetatype == 'C' || $oldmetatype == 'X') &&
                $xmldb_field->getType() == XMLDB_TYPE_INTEGER) {
                $alterstmt .= ' CAST(CAST('.$fieldname.' AS NUMERIC) AS INTEGER)'; // From char to integer
            } else if ($typechanged) {
                $alterstmt .= $fieldname. '::' . $this->getFieldSQL($xmldb_table, $xmldb_field, null, true, true, null, false);
            }
            else {
                $alterstmt .= $fieldname;
            }
            $results[] = $alterstmt;

            $results[] = 'ALTER TABLE '. $tablename . ' DROP COLUMN ' . $fieldname;
            $results[] = 'ALTER TABLE '. $tablename . ' RENAME COLUMN ' . $tempcolumnnname . ' TO ' . $fieldname;

            # todo: we need to deal with updates that don't work, ie invalid entries in tables
            # Somehow we need to get the info up to dbmanager/native cockroachdb
            # that we are doing a column update, it won't be transactional 
            # and do some error handling.
        }

        // If the default has changed or we have performed one change in specs
        if ($defaultchanged || $specschanged) {
            $default_clause = $this->getDefaultClause($xmldb_field);
            if ($default_clause) {
                $sql = 'ALTER TABLE ' . $tablename . ' ALTER COLUMN ' . $fieldname . ' SET' . $default_clause;     // Add default clause
                $results[] = $sql;
            } else {
                if (!$specschanged) {     // Only drop default if we haven't performed one specs change
                    $results[] = 'ALTER TABLE ' . $tablename . ' ALTER COLUMN ' . $fieldname . ' DROP DEFAULT';     // Drop default clause
                }
            }
        }

        // If the not null has changed
        if ($notnullchanged) {
            if ($xmldb_field->getNotnull()) {
                $results[] = 'ALTER TABLE ' . $tablename . ' ALTER COLUMN ' . $fieldname . ' SET NOT NULL';
            } else {
                $results[] = 'ALTER TABLE ' . $tablename . ' ALTER COLUMN ' . $fieldname . ' DROP NOT NULL';
            }
        }

        return $results;
    }

    /**
     * Returns the code needed to drop one sequence for the xmldb_table and xmldb_field passed
     * Can, optionally, specify if the underlying trigger will be also dropped
     *
     * @param xmldb_table $xmldb_table The xmldb_table object instance.
     * @param xmldb_field $xmldb_field The xmldb_field object instance.
     * @param bool $include_trigger
     * @return array Array of SQL statements to create the sequence.
     */
    public function getDropSequenceSQL($xmldb_table, $xmldb_field, $include_trigger=false) {
        if ($sequence_name = $this->getSequenceFromDB($xmldb_table)) {
            $result[] = "DROP SEQUENCE " . $sequence_name;
            return $result;
        }
        else {
            return array();
        }
        
    }

    /**
     * Returns the code (array of statements) needed to execute extra statements on table drop
     *
     * @param xmldb_table $xmldb_table The xmldb_table object instance.
     * @return array Array of extra SQL statements to drop a table.
     */
    public function getDropTableExtraSQL($xmldb_table) {
        $xmldb_field = new xmldb_field('id'); // Fields having sequences should be exclusively, id.
        return $this->getDropSequenceSQL($xmldb_table, $xmldb_field, false);
    }

    /**
     * Cockroachdb doesn't support temptables yet
     * https://github.com/cockroachdb/cockroach/issues/5807
     *
     * Create a regular table and rely on the base sql_generator to clean up.
     * 
     * @param xmldb_table $xmldb_table The xmldb_table object instance.
     * @return array of sql statements
     */
    public function getCreateTempTableSQL($xmldb_table) {
        $this->temptables->add_temptable($xmldb_table->getName());
        $sqlarr = $this->getCreateTableSQL($xmldb_table);
        return $sqlarr;
    }


    /**
     * Given one correct xmldb_index, returns the SQL statements
     * needed to create it (in array).
     *
     * @param xmldb_table $xmldb_table The xmldb_table instance to create the index on.
     * @param xmldb_index $xmldb_index The xmldb_index to create.
     * @return array An array of SQL statements to create the index.
     * @throws coding_exception Thrown if the xmldb_index does not validate with the xmldb_table.
     */
    public function getCreateIndexSQL($xmldb_table, $xmldb_index) {
        # cockroachdb does not support varchar_pattern_ops
        $sqls = sql_generator::getCreateIndexSQL($xmldb_table, $xmldb_index);
        return $sqls;
    }
}