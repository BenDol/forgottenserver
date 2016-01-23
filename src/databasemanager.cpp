/**
 * The Forgotten Server - a free and open-source MMORPG server emulator
 * Copyright (C) 2016  Mark Samman <mark.samman@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include "otpch.h"

#include "configmanager.h"
#include "databasemanager.h"
#include "luascript.h"

#include <iostream>
#include <fstream>

#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>

extern ConfigManager g_config;

bool DatabaseManager::optimizeTables()
{
	Database* db = Database::getInstance();
	std::ostringstream query;

	query << "SELECT `TABLE_NAME` FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = " << db->escapeString(g_config.getString(ConfigManager::MYSQL_DB)) << " AND `DATA_FREE` > 0";
	DBResult_ptr result = db->storeQuery(query.str());
	if (!result) {
		return false;
	}

	do {
		std::string tableName = result->getString("TABLE_NAME");
		std::cout << "> Optimizing table " << tableName << "..." << std::flush;

		query.str(std::string());
		query << "OPTIMIZE TABLE `" << tableName << '`';

		if (db->executeQuery(query.str())) {
			std::cout << " [success]" << std::endl;
		} else {
			std::cout << " [failed]" << std::endl;
		}
	} while (result->next());
	return true;
}

bool DatabaseManager::tableExists(const std::string& tableName)
{
	Database* db = Database::getInstance();

	std::ostringstream query;
	query << "SELECT `TABLE_NAME` FROM `information_schema`.`tables` WHERE `TABLE_SCHEMA` = " << db->escapeString(g_config.getString(ConfigManager::MYSQL_DB)) << " AND `TABLE_NAME` = " << db->escapeString(tableName) << " LIMIT 1";
	return db->storeQuery(query.str()).get() != nullptr;
}

bool DatabaseManager::isDatabaseSetup()
{
	Database* db = Database::getInstance();
	std::ostringstream query;
	query << "SELECT `TABLE_NAME` FROM `information_schema`.`tables` WHERE `TABLE_SCHEMA` = " << db->escapeString(g_config.getString(ConfigManager::MYSQL_DB));
	return db->storeQuery(query.str()).get() != nullptr;
}

int32_t DatabaseManager::getDatabaseVersion()
{
	if (!tableExists("server_config")) {
		return 0;
	}

	int32_t version = 0;
	if (getDatabaseConfig("db_version", version)) {
		return version;
	}
	return -1;
}

int32_t DatabaseManager::getDatabaseSQLVersion()
{
	if (!tableExists("server_config")) {
		Database* db = Database::getInstance();
		db->executeQuery("CREATE TABLE `server_config` (`config` VARCHAR(50) NOT nullptr, `value` VARCHAR(256) NOT nullptr DEFAULT '', UNIQUE(`config`)) ENGINE = InnoDB");
		db->executeQuery("INSERT INTO `server_config` VALUES ('db_sql_version', 0)");
		return 0;
	}

	int32_t version = 0;
	if (getDatabaseConfig("db_sql_version", version)) {
		return version;
	}
	return -1;
}

void DatabaseManager::updateDatabase()
{
	lua_State* L = luaL_newstate();
	if (!L) {
		return;
	}

	luaL_openlibs(L);

#ifndef LUAJIT_VERSION
	//bit operations for Lua, based on bitlib project release 24
	//bit.bnot, bit.band, bit.bor, bit.bxor, bit.lshift, bit.rshift
	luaL_register(L, "bit", LuaScriptInterface::luaBitReg);
#endif

	//db table
	luaL_register(L, "db", LuaScriptInterface::luaDatabaseTable);

	//result table
	luaL_register(L, "result", LuaScriptInterface::luaResultTable);

	int32_t version = getDatabaseVersion();
	do {
		std::ostringstream ss;
		ss << "data/migrations/" << version << ".lua";
		if (luaL_dofile(L, ss.str().c_str()) != 0) {
			std::cout << "[Error - DatabaseManager::updateDatabase - Version: " << version << "] " << lua_tostring(L, -1) << std::endl;
			break;
		}

		if (!LuaScriptInterface::reserveScriptEnv()) {
			break;
		}

		lua_getglobal(L, "onUpdateDatabase");
		if (lua_pcall(L, 0, 1, 0) != 0) {
			LuaScriptInterface::resetScriptEnv();
			std::cout << "[Error - DatabaseManager::updateDatabase - Version: " << version << "] " << lua_tostring(L, -1) << std::endl;
			break;
		}

		if (!LuaScriptInterface::getBoolean(L, -1, false)) {
			LuaScriptInterface::resetScriptEnv();
			break;
		}

		version++;
		std::cout << "> Database has been updated to version " << version << '.' << std::endl;
		registerDatabaseConfig("db_version", version);

		LuaScriptInterface::resetScriptEnv();
	} while (true);
	lua_close(L);
}

int32_t DatabaseManager::applySQLMigrations()
{
	Database* db = Database::getInstance();
	int32_t version = getDatabaseSQLVersion();
	if(version < 0) {
		return 0;
	}

	std::string dir = "db/migrations/";
	StringVec files = loadMigrationFiles(dir);

	std::sort(files.begin(), files.end());
	for(StringVec::iterator it = files.begin(); it != files.end(); ++it) {
		std::string file = (*it).substr(1,1);
		if(isNumbers(file)) {
			int32_t mversion = atoi(file.c_str());
			if(mversion > version) {
				std::ifstream ifs;
				ifs.open(dir + (*it), std::ifstream::in);

				// get length of file:
				ifs.seekg (0, ifs.end);
				int32_t length = ifs.tellg();
				ifs.seekg (0, ifs.beg);

				char* buffer = new char [length];
				ifs.read(buffer,length);
				ifs.close();

				std::clog << "> Migrating to database version ." << file << "...";

				std::string content = db->escapeBlob(buffer, length);
				replaceString(content, "\\n", " ");
				replaceString(content, "\t", "");
				replaceString(content, "\\", "");
				stripUnicode(content);
				content = content.substr(1, content.length()-2);
				boost::algorithm::trim(content);

				bool passed = true;

				StringVec triggers = gatherMigrationTriggers(content);
				StringVec queries = split(content, ';');

				delete[] buffer;

				// Start the migration file transaction
				if(db->beginTransaction()) {
					// Process the migration queries
					for(StringVec::iterator itt = queries.begin(); itt != queries.end(); ++itt) {
						std::string query = (*itt);
						boost::algorithm::trim(query);
						if(!query.empty() && !db->executeQuery(query + ";")) {
							passed = false;
							break;
						}
					}
					// Process the migration triggers
					if(passed) {
						for(StringVec::iterator itt = triggers.begin(); itt != triggers.end(); ++itt) {
							std::string trigger = (*itt);
							boost::algorithm::trim(trigger);
							if(!trigger.empty() && !db->executeQuery(trigger + ";")) {
								passed = false;
								break;
							}
						}
					}

					if(passed) {
						version = mversion;

						registerDatabaseConfig("db_sql_version", version);
						if(db->commit())
							std::clog << " Success!" << std::endl;
						else
							db->rollback();
					} else {
						std::clog << " Failed";
						if(db->rollback())
							std::clog << ", rolled back." << std::endl;
						else
							std::clog << std::endl;
					}
				}
			}
		}
	}
	return version;
}

StringVec DatabaseManager::gatherMigrationTriggers(std::string& content) {
	StringVec triggers;

	std::size_t delimiterStart = content.find("DELIMITER |");
	if(delimiterStart != std::string::npos) {
		std::size_t delimiterEnd = content.find("DELIMITER ;");
		if(delimiterEnd != std::string::npos) {
			std::string triggerStr = content.substr(delimiterStart+11, delimiterEnd-(delimiterStart+11));
			boost::algorithm::trim(triggerStr);
			triggers = split(triggerStr, '|');

			// Remove the triggers from the content
			content = content.replace(delimiterStart, (delimiterEnd+11)-delimiterStart, "");
		} else {
			std::clog << "Failed to find the end DELIMITER placeholder.";
		}
	}
	return triggers;
}

StringVec DatabaseManager::loadMigrationFiles(std::string dir) {
	StringVec files;

	for(boost::filesystem::directory_iterator it(dir), end; it != end; ++it) {
		std::string s = (it)->path().filename().string();

		if((s.size() > 4 ? s.substr(s.size() - 4) : "") == ".sql")
			files.push_back(s);
	}
	return files;
}

bool DatabaseManager::getDatabaseConfig(const std::string& config, int32_t& value)
{
	Database* db = Database::getInstance();
	std::ostringstream query;
	query << "SELECT `value` FROM `server_config` WHERE `config` = " << db->escapeString(config);

	DBResult_ptr result = db->storeQuery(query.str());
	if (!result) {
		return false;
	}

	value = result->getNumber<int32_t>("value");
	return true;
}

void DatabaseManager::registerDatabaseConfig(const std::string& config, int32_t value)
{
	Database* db = Database::getInstance();
	std::ostringstream query;

	int32_t tmp;

	if (!getDatabaseConfig(config, tmp)) {
		query << "INSERT INTO `server_config` VALUES (" << db->escapeString(config) << ", '" << value << "')";
	} else {
		query << "UPDATE `server_config` SET `value` = '" << value << "' WHERE `config` = " << db->escapeString(config);
	}

	db->executeQuery(query.str());
}
