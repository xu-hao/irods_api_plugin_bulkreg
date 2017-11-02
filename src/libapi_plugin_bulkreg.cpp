// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "irods_stacktrace.hpp"
#include "irods_server_api_call.hpp"
#include "irods_re_serialization.hpp"
#include "irods_database_object.hpp"
#include "irods_database_factory.hpp"
#include "irods_database_manager.hpp"
#include "irods_database_constants.hpp"
#include "boost/lexical_cast.hpp"
#include "avro/Decoder.hh"

#include "objStat.h"
#include "rodsPackInstruct.h"
#include "irods_database_plugin_cockroachdb_constants.hpp"
#include "irods_database_plugin_cockroachdb_structs.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>

int call_bulkreg(
    irods::api_entry* _api,
    rsComm_t*         _comm,
    void*       _inp,
    int* _out) {
    return _api->call_handler<
               void *,
               int *>(
                   _comm,
                   _inp,
                   _out);
}

#ifdef RODS_SERVER
    static irods::error serialize_void_ptr(
                    boost::any               _p,
                    irods::re_serialization::serialized_parameter_t& _out) { 
                try {
                    boost::any_cast<void*>(_p);
                }
                catch ( std::exception& ) {
                    return ERROR(
                             INVALID_ANY_CAST,
                             "failed to cast XXXX ptr" );
                }

                return SUCCESS();
            } // serialize_XXXX_ptr
    #define CALL_BULKREG call_bulkreg
#else
    #define CALL_BULKREG NULL
#endif

// =-=-=-=-=-=-=-
// api function to be referenced by the entry
int rs_bulkreg( rsComm_t* _comm, void *_ptr, int* _out ) {
    rodsLog( LOG_NOTICE, "Dynamic API - BULKREG" );
    bytesBuf_t *bb = reinterpret_cast<bytesBuf_t *>(_ptr);
    auto in = avro::memoryInputStream(reinterpret_cast<uint8_t *>(bb->buf), bb->len);
    auto dec = avro::binaryDecoder();
    
    dec->init(*in);
    irods::Bulk bulk;
    avro::decode(*dec, bulk);
    
    // =-=-=-=-=-=-=-
    // call factory for database object
    irods::database_object_ptr db_obj_ptr;
    irods::error ret = irods::database_factory(
                           "cockroachdb",
                           db_obj_ptr );
    if ( !ret.ok() ) {
        irods::log( PASS( ret ) );
        return ret.code();
    }

    // =-=-=-=-=-=-=-
    // resolve a plugin for that object
    irods::plugin_ptr db_plug_ptr;
    ret = db_obj_ptr->resolve(
              irods::DATABASE_INTERFACE,
              db_plug_ptr );
    if ( !ret.ok() ) {
        irods::log(
            PASSMSG(
                "failed to resolve database interface",
                ret ) );
        return ret.code();
    }

    // =-=-=-=-=-=-=-
    // cast plugin and object to db and fco for call
    irods::first_class_object_ptr ptr = boost::dynamic_pointer_cast <
                                        irods::first_class_object > ( db_obj_ptr );
    irods::database_ptr           db = boost::dynamic_pointer_cast <
                                       irods::database > ( db_plug_ptr );

    // =-=-=-=-=-=-=-
    // call the get local zone operation on the plugin
    ret = db->call <
          irods::Bulk* > ( _comm,
                                 irods::DATABASE_OP_BULKREG,
                                 ptr,
                                 &bulk );

    *_out = 0;

    rodsLog( LOG_NOTICE, "Dynamic API - DONE" );

    return ret.code();
}

extern "C" {
    // =-=-=-=-=-=-=-
    // factory function to provide instance of the plugin
    irods::api_entry* plugin_factory(
        const std::string&,     //_inst_name
        const std::string& ) { // _context
        // =-=-=-=-=-=-=-
        // create a api def object
        irods::apidef_t def = { 30000,             // api number
                                RODS_API_VERSION, // api version
                                REMOTE_USER_AUTH,      // client auth
                                REMOTE_PRIV_USER_AUTH,     // proxy auth
                                "BytesBuf_PI", 0, // in PI / bs flag
                                "INT_PI", 0, // out PI / bs flag
                                std::function<
                                    int( rsComm_t*, void*, int*)>(
                                        rs_bulkreg), // operation
								"api_hello_world",    // operation name
                                0,  // null clear fcn
                                (funcPtr)CALL_BULKREG
                              };
        // =-=-=-=-=-=-=-
        // create an api object
        irods::api_entry* api = new irods::api_entry( def );

#ifdef RODS_SERVER
        irods::re_serialization::add_operation(
                typeid(void*),
                serialize_void_ptr );
#endif

        // =-=-=-=-=-=-=-
        // assign the pack struct key and value
        api->in_pack_key   = "BytesBuf_PI";
        api->in_pack_value = BytesBuf_PI;

        api->out_pack_key   = "INT_PI";
        api->out_pack_value = INT_PI;

        return api;

    } // plugin_factory

}; // extern "C"