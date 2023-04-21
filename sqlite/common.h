#pragma once

const std::string PROP_UDP_PORT_CLI = "sqlite.udp_port_cli";
const std::string PROP_UDP_PORT_CLI_DEFAULT = "31850";

const std::string PROP_UDP_PORT_SVR = "sqlite.udp_port_svr";
const std::string PROP_UDP_PORT_SVR_DEFAULT = "31850";

const std::string PROP_CLI_HOSTNAME = "sqlite.client_hostname";
const std::string PROP_CLI_HOSTNAME_DEFAULT = "localhost";

const std::string PROP_SVR_HOSTNAME = "sqlite.server_hostname";
const std::string PROP_SVR_HOSTNAME_DEFAULT = "localhost";

const std::string PROP_MSG_SIZE = "sqlite.msg_size";
const std::string PROP_MSG_SIZE_DEFAULT = "1024";

const std::string PROP_NAME = "sqlite.dbname";
const std::string PROP_NAME_DEFAULT = "";

const uint8_t SQL_READ_REQ    = 0;
const uint8_t SQL_UPDATE_REQ  = 1;
const uint8_t SQL_INSERT_REQ  = 2;
const uint8_t SQL_DELETE_REQ  = 3;
const uint8_t SQL_SCAN_REQ    = 4;