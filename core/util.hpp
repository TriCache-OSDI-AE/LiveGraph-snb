#pragma once
#include <chrono>
#include <iomanip>
#include <sstream>
#include "date/include/date/tz.h"

std::chrono::system_clock::time_point static inline from_date(const std::string &str)
{
    const std::string date_parse("%Y-%m-%d");
    std::tm tm{};
    std::stringstream ss(str);
    ss >> std::get_time(&tm, date_parse.c_str());
    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}

std::chrono::system_clock::time_point static inline from_time(const std::string &str)
{
    const std::string time_parse("%Y-%m-%dT%H:%M:%S.");
    std::tm tm{};
    std::stringstream ss(str);
    int ms, zone;
    char c;
    ss >> std::get_time(&tm, time_parse.c_str()) >> ms >> c >> zone;
    if(c == '+') tm.tm_hour += zone;
    if(c == '-') tm.tm_hour -= zone;
    return std::chrono::system_clock::from_time_t(std::mktime(&tm)) + std::chrono::milliseconds(ms);
}

uint64_t static inline to_time(uint32_t date)
{
    auto tp = std::chrono::system_clock::time_point(std::chrono::hours(date));
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}

std::pair<int, int> static inline to_monday(uint64_t date)
{
    auto tp = std::chrono::system_clock::time_point(std::chrono::hours(date));
    auto dp = date::floor<date::days>(tp);
    auto ymd = date::year_month_day(dp);
    int month = (unsigned)ymd.month();
    int day = (unsigned)ymd.day();
    return std::make_pair(month, day);
}

std::vector<std::string> static inline split(const std::string &s, char delim) {
    std::stringstream ss(s);
    std::string item;
    std::vector<std::string> elems;
    while (std::getline(ss, item, delim)) {
        elems.push_back(std::move(item));
    }
    return elems;
}

const static char csv_split('|');
const static char list_split(';');
const static char zero_split('\000');

