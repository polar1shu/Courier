#pragma once

#include <iostream>
#include <iomanip>
#include <format>
#include <string_view>

namespace util {

    inline void print_property_header(std::string_view header_name) {
        std::cout   << "\e[33;1m"
                    << std::format(" [ {} ]--------------------------------------------------------------------", header_name) 
                    << "\e[0m"
                    << std::endl;
    }


    inline void print_kv_pair() { }

    template<class TupleType, class ...Args>
    inline void print_kv_pair(const TupleType &tuple, Args &&...args) {
        auto [key, value, unit] = tuple;
        std::cout << std::left << std::format("{:36}\t{:36}\t{}", key, value, unit) << '\n';
        print_kv_pair(std::forward<Args>(args)...);
    }


    template<class ...Args>
    inline void print_property(std::string_view header_name, Args &&...args) {
        print_property_header(header_name);

        std::cout << "\e[32m";
        print_kv_pair(std::forward<Args>(args)...);
        std::cout << "\e[0m" << std::endl;
    }

}