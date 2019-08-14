//
// Created by kyle on 8/12/19.
//
#include <cpprest/http_client.h>
#include <cpprest/filestream.h>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <fstream>
#include <random>
#include <set>
#include "cpprest/json.h"
#include "cpprest/http_listener.h"
#include "cpprest/uri.h"
#include "cpprest/asyncrt_utils.h"
#include <map>
#include <cpprest/details/basic_types.h>

#ifdef _WIN32
#ifdef NOMINMAX
#define NOMINMAX
#endif
#include <Windows.h>
#else
#include <sys/time.h>
#endif

using namespace std;
using namespace web;
using namespace utility;
using namespace http;
using namespace web::http::experimental::listener;


void DisplayJSON(json::value const & jvalue){
    cout << jvalue.serialize() << endl;
}

void RequestWorker(http_request& request, function<void(json::value const &, json::value &)> handler)
{
    auto result = json::value::object();
    request.extract_json().then([&result, &handler](pplx::task<json::value> task) {
        try{
            auto const & jvalue = task.get();
            if(!jvalue.is_null())
                handler(jvalue, result);
        }
        catch(http_exception const & e) {
            cout << "Exception -> " << e.what() << endl;
        }
    }).wait();
    request.reply(status_codes::OK, result);
}

/*
 * Mock DB which simulates a key/value DB
 */
class HttpKeyValueDBEngine {
    map<utility::string_t, utility::string_t> storage;
public:
    HttpKeyValueDBEngine() {
        storage["John"] = "20";
        storage["Paul"] = "21";
        storage["George"] = "22";
    }

    void GET_HANDLER(http_request &request) {
        auto resp_obj = json::value::object();
        for (auto const &p : storage)

            resp_obj[p.first] = json::value::string(p.second);
        request.reply(status_codes::OK, resp_obj);
    }

    void POST_HANDLER(http_request &request) {
        RequestWorker(request, [&](json::value const &jvalue, json::value &result) {
            //write to console for diagnostics
            DisplayJSON(jvalue);
            for (auto const &e : jvalue.as_array()) {
                if (e.is_string()) {
                    auto key = e.as_string();
                    auto pos = storage.find(key);
                    if (pos == storage.end()) {
                        //indicate to client that the key isnt found
                        result[key] = json::value::string("notfound");
                    } else {
                        //store the kv pair in the result json, the rsult will be sent
                        //back to client
                        result[pos->first] = json::value::string(pos->second);
                    }
                }
            }
        });
    }

    void PUT_HANDLER(http_request &request) {
        RequestWorker(request, [&](json::value const &jvalue, json::value &result) {
            DisplayJSON(jvalue);
            for (auto const &e : jvalue.as_object()) {
                if (e.second.is_string()) {
                    auto key = e.first;
                    auto value = e.second.as_string();
                    if (storage.find(key) == storage.end()) {
                        //let client know we created new record
                        result[key] = json::value::string("<put");
                    } else {
                        //let client know we updated a record
                        result[key] = json::value::string("<updated>");
                    }
                    storage[key] = value;
                }
            }
        });
    }

    void DEL_HANDLER(http_request& request)
    {
        RequestWorker(request, [&](json::value const & jvalue, json::value & result)
        {
            //aggregate all keys into this set & delete in one go
            set<utility::string_t>keys;
            for(auto const & e : jvalue.as_array()){
                if(e.is_string()){
                    auto key = e.as_string();
                    auto pos = storage.find(key);
                    if (pos == storage.end()){
                        result[key] = json::value::string("<failed>");
                    }
                    else{
                        result[key] = json::value::string("<deleted>");
                        keys.insert(key);
                    }
                }
            }
            //erase all
            for(auto const & key : keys)
                storage.erase(key);
        });
    }
};

//Instantiates global instance of k/v DB
HttpKeyValueDBEngine g_dbengine;