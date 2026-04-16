#ifndef MINIREDIS_CONFIGUREPARSE_HPP
#define MINIREDIS_CONFIGUREPARSE_HPP

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <string>
#include <cctype>
#include <optional>

namespace MiniRedis {

    namespace ConfigureParser {

        using ConfigureMap = std::map<std::string, std::vector<std::vector<std::string>>>;

        static void trim(std::string& s) {
            size_t start = 0;
            while (start < s.size() && std::isspace(s[start])) start++;

            size_t end = s.size();
            while (end > start && std::isspace(s[end - 1])) end--;

            s = s.substr(start, end - start);
        }

        static bool splitArgs(const std::string& line, std::vector<std::string>& out) {
            std::string current;
            bool inQuotes = false;
            bool escape = false;

            for (size_t i = 0; i < line.size(); ++i) {
                char c = line[i];
                if (escape) {
                    switch (c) {
                        case 'n': current += '\n'; break;
                        case 't': current += '\t'; break;
                        case 'r': current += '\r'; break;
                        case '\\': current += '\\'; break;
                        case '"': current += '"'; break;
                        default: current += c; break;
                    }
                    escape = false;
                    continue;
                }

                if (c == '\\') {
                    escape = true;
                    continue;
                }

                if (c == '"') {
                    inQuotes = !inQuotes;
                    continue;
                }

                if (std::isspace(c) && !inQuotes) {
                    if (!current.empty()) {
                        out.push_back(current);
                        current.clear();
                    }
                    continue;
                }

                current += c;
            }

            if (escape || inQuotes) {
                return false;
            }

            if (!current.empty()) {
                out.push_back(current);
            }

            return true;
        }

        static bool parse(const std::string_view& filename, ConfigureMap& outConfig,std::string& errorDetail) {
            std::ifstream file(filename.data());
            if (!file.is_open()) {
                errorDetail = "Failed to open config file";
                return false;
            }

            std::string line;
            int lineNumber = 0;

            while (std::getline(file, line)) {
                lineNumber++;

                trim(line);

                if (line.empty() || line[0] == '#') {
                    continue;
                }

                std::vector<std::string> argv;
                if (!splitArgs(line, argv)) {
                    errorDetail = "Parse error at line " + std::to_string(lineNumber);
                    return false;
                }

                if (argv.empty()) continue;

                const std::string& key = argv[0];
                std::vector<std::string> values(argv.begin() + 1, argv.end());

                outConfig[key].emplace_back(std::move(values));
            }

            return true;
        }

    };

}

#endif //MINIREDIS_CONFIGUREPARSE_HPP