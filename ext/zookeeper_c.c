/* Ruby wrapper for the ZooKeeper C API
 * Phillip Pearson <pp@myelin.co.nz>
 * Eric Maland <eric@twitter.com>
 */

#define THREADED

#include "ruby.h"

#include "c-client-src/zookeeper.h"
#include <errno.h>
#include <stdio.h>

static VALUE ZooKeeper = Qnil;
static VALUE eNoNode = Qnil;
static VALUE eBadVersion = Qnil;

struct zk_rb_data {
  zhandle_t *zh;
  clientid_t myid;
};

static void watcher(zhandle_t *zh, int type, int state, const char *path, void *ctx) {
  VALUE self, watcher_id;
  (void)ctx;
  return; // watchers don't work in ruby yet

  self = (VALUE)zoo_get_context(zh);;
  watcher_id = rb_intern("watcher");

  fprintf(stderr,"C watcher %d state = %d for %s.\n", type, state, (path ? path: "null"));
  rb_funcall(self, watcher_id, 3, INT2FIX(type), INT2FIX(state), rb_str_new2(path));
}

#warning [emaland] incomplete - but easier to read!
static void check_errors(int rc) {
  switch (rc) {
  case ZOK: 
    /* all good! */ 
    break;
  case ZNONODE: 
    rb_raise(eNoNode, "the node does not exist");
    break;
  case ZBADVERSION: 
    rb_raise(eBadVersion, "expected version does not match actual version");
    break;
  default: 
    rb_raise(rb_eRuntimeError, "unknown error returned from zookeeper: %d (%s)", rc, zerror(rc));
  }
}

static void free_zk_rb_data(struct zk_rb_data* ptr) {
  zookeeper_close(ptr->zh);
}

static VALUE array_from_stat(const struct Stat* stat) {
  return rb_ary_new3(8,
		     LL2NUM(stat->czxid),
		     LL2NUM(stat->mzxid),
		     LL2NUM(stat->ctime),
		     LL2NUM(stat->mtime),
		     INT2NUM(stat->version),
		     INT2NUM(stat->cversion),
		     INT2NUM(stat->aversion),
		     LL2NUM(stat->ephemeralOwner));
}

static VALUE method_initialize(VALUE self, VALUE hostPort) {
  VALUE data;
  struct zk_rb_data* zk = NULL;

  Check_Type(hostPort, T_STRING);

  data = Data_Make_Struct(ZooKeeper, struct zk_rb_data, 0, free_zk_rb_data, zk);

  zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
  zoo_deterministic_conn_order(0);

  zk->zh = zookeeper_init(RSTRING(hostPort)->ptr, watcher, 10000, &zk->myid, (void*)self, 0);
  if (!zk->zh) {
    rb_raise(rb_eRuntimeError, "error connecting to zookeeper: %d", errno);
  }

  rb_iv_set(self, "@data", data);

  return Qnil;
}

#define FETCH_DATA_PTR(x, y) \
  struct zk_rb_data * y; \
  Data_Get_Struct(rb_iv_get(x, "@data"), struct zk_rb_data, y)

static VALUE method_get_children(VALUE self, VALUE path) {
  struct String_vector strings;
  int i;
  VALUE output;

  Check_Type(path, T_STRING);
  FETCH_DATA_PTR(self, zk);

  check_errors(zoo_get_children(zk->zh, RSTRING(path)->ptr, 0, &strings));

  output = rb_ary_new();
  for (i = 0; i < strings.count; ++i) {
    rb_ary_push(output, rb_str_new2(strings.data[i]));
  }
  return output;
}

static VALUE method_exists(VALUE self, VALUE path, VALUE watch) {
  struct Stat stat;

  Check_Type(path, T_STRING);
  FETCH_DATA_PTR(self, zk);

  check_errors(zoo_exists(zk->zh, RSTRING(path)->ptr, (watch != Qfalse && watch != Qnil), &stat));

  return array_from_stat(&stat);
}

static VALUE method_create(VALUE self, VALUE path, VALUE value, VALUE flags) {
  char realpath[10240];

  Check_Type(path, T_STRING);
  Check_Type(value, T_STRING);
  Check_Type(flags, T_FIXNUM);

  FETCH_DATA_PTR(self, zk);

  check_errors(zoo_create(zk->zh, RSTRING(path)->ptr, 
                          RSTRING(value)->ptr, RSTRING(value)->len,
			  &ZOO_OPEN_ACL_UNSAFE, FIX2INT(flags), 
                          realpath, sizeof(realpath)));

  return rb_str_new2(realpath);
}

static VALUE method_delete(VALUE self, VALUE path, VALUE version) {
  Check_Type(path, T_STRING);
  Check_Type(version, T_FIXNUM);

  FETCH_DATA_PTR(self, zk);

  check_errors(zoo_delete(zk->zh, RSTRING(path)->ptr, FIX2INT(version)));

  return Qtrue;
}

static VALUE method_get(VALUE self, VALUE path) {
  char data[1024];
  int data_len = sizeof(data);

  struct Stat stat;
  memset(data, 0, sizeof(data));

  Check_Type(path, T_STRING);
  FETCH_DATA_PTR(self, zk);
  
  check_errors(zoo_get(zk->zh, RSTRING(path)->ptr, 0, data, &data_len, &stat));

  return rb_ary_new3(2,
		     rb_str_new(data, data_len),
		     array_from_stat(&stat));
}

static VALUE method_set(int argc, VALUE* argv, VALUE self)
{
  VALUE v_path, v_data, v_version;
  int real_version = -1;

  FETCH_DATA_PTR(self, zk);

  rb_scan_args(argc, argv, "21", &v_path, &v_data, &v_version);
  
  Check_Type(v_path, T_STRING);
  Check_Type(v_data, T_STRING);
  Check_Type(v_version, T_FIXNUM);

  if(!NIL_P(v_version))
    real_version = FIX2INT(v_version);

  check_errors(zoo_set(zk->zh, 
                       RSTRING(v_path)->ptr, 
                       RSTRING(v_data)->ptr, RSTRING(v_data)->len, 
                       FIX2INT(v_version)));

  return Qtrue;
}

static void void_completion_callback(int rc, const void *data) {

}

static void string_completion_callback(int rc, const char *value, const void *data) {
  
}

#warning [emaland] to be implemented
static VALUE method_set2(int argc, VALUE *argv, VALUE self) {
  //  ZOOAPI int zoo_set2(zhandle_t *zh, const char *path, const char *buffer,
  //                    int buflen, int version, struct Stat *stat);
  return Qnil;

}

static VALUE method_set_acl(int argc, VALUE* argv, VALUE self) {
/* STUB */
/*   VALUE v_path, v_data, v_version; */
/*   struct zk_rb_data* zk; */
/*   int real_version = -1; */

/*   rb_scan_args(argc, argv, "21", &v_path, &v_data, &v_version); */
  
/*   Check_Type(v_path, T_STRING); */
/*   Check_Type(v_data, T_STRING); */
/*   Check_Type(v_version, T_FIXNUM); */

/*   if(!NIL_P(v_version)) */
/*     real_version = FIX2INT(v_version); */

/*   Data_Get_Struct(rb_iv_get(self, "@data"), struct zk_rb_data, zk); */
  
/*   check_errors(zoo_set(zk->zh, RSTRING(v_path)->ptr,  */
/*                        RSTRING(v_data)->ptr, RSTRING(v_data)->len,  */
/*                        FIX2INT(v_version))); */

  return Qnil;
}

/*
        PARAMETERS:
         zh: the zookeeper handle obtained by a call to zookeeper.init
         scheme: the id of authentication scheme. Natively supported:
        'digest' password-based authentication
         cert: application credentials. The actual value depends on the scheme.
         completion: the routine to invoke when the request completes. One of 
        the following result codes may be passed into the completion callback:
        OK operation completed successfully
        AUTHFAILED authentication failed 
        
        RETURNS:
        OK on success or one of the following errcodes on failure:
        BADARGUMENTS - invalid input parameters
        INVALIDSTATE - zhandle state is either SESSION_EXPIRED_STATE or AUTH_FAI
LED_STATE
        MARSHALLINGERROR - failed to marshall a request; possibly, out of memory
        SYSTEMERROR - a system error occured
*/
#warning [emaland] make these magically synchronous for now?
static VALUE method_add_auth(VALUE self, VALUE scheme, 
                             VALUE cert, VALUE completion, 
                             VALUE completion_data) {
  struct zk_rb_data* zk;
  Data_Get_Struct(rb_iv_get(self, "@data"), struct zk_rb_data, zk);

  Check_Type(scheme, T_STRING);
  Check_Type(cert, T_STRING);
  //  Check_Type(completion, T_OBJECT); // ???
  
  check_errors(zoo_add_auth(zk->zh, RSTRING(scheme)->ptr,
                            RSTRING(cert)->ptr, RSTRING(cert)->len,
                            void_completion_callback, DATA_PTR(completion_data)));
  return Qtrue;
}

static VALUE method_async(VALUE self, VALUE path, 
                          VALUE completion, VALUE completion_data) {
  struct zk_rb_data* zk;
  Data_Get_Struct(rb_iv_get(self, "@data"), struct zk_rb_data, zk);

  Check_Type(path, T_STRING);
  //  Check_Type(completion, T_OBJECT); // ???
  
  check_errors(zoo_async(zk->zh, RSTRING(path)->ptr,
                         string_completion_callback, DATA_PTR(completion_data)));

  return Qtrue;
}

static VALUE method_client_id(VALUE self) {
  FETCH_DATA_PTR(self, zk);
  const clientid_t *id = zoo_client_id(zk->zh);
  return UINT2NUM(id->client_id);
}

static VALUE method_close(VALUE self) {
  FETCH_DATA_PTR(self, zk);
  check_errors(zookeeper_close(zk->zh));
  return Qtrue;
}

static VALUE method_deterministic_conn_order(VALUE self, VALUE yn) {
  zoo_deterministic_conn_order(yn == Qtrue);
  return Qnil;
}

static VALUE id_to_ruby(struct Id *id) {
  VALUE hash = rb_hash_new();
  rb_hash_aset(hash, rb_str_new2("scheme"), rb_str_new2(id->scheme));
  rb_hash_aset(hash, rb_str_new2("id"), rb_str_new2(id->id));
  return hash;
}

static VALUE acl_to_ruby(struct ACL *acl) {
  VALUE hash = rb_hash_new();
  rb_hash_aset(hash, rb_str_new2("perms"), INT2NUM(acl->perms));
  rb_hash_aset(hash, rb_str_new2("id"), id_to_ruby(&(acl->id)));
  return hash;
}

static VALUE acl_vector_to_ruby(struct ACL_vector *acl_vector) {
  int i = 0;
  VALUE ary = rb_ary_new();
  for(i = 0; i < acl_vector->count; i++) {
    rb_ary_push(ary, acl_to_ruby(acl_vector->data+i));
  }
  return ary;
}

/*
  struct Stat {
    int64_t czxid;
    int64_t mzxid;
    int64_t ctime;
    int64_t mtime;
    int32_t version;
    int32_t cversion;
    int32_t aversion;
    int64_t ephemeralOwner;
    int32_t dataLength;
    int32_t numChildren;
    int64_t pzxid;
  }
}
*/
static VALUE stat_to_ruby(struct Stat *stat) {
  VALUE hash = rb_hash_new();
  rb_hash_aset(hash, rb_str_new2("czxid"), UINT2NUM(stat->czxid));
  rb_hash_aset(hash, rb_str_new2("mzxid"), UINT2NUM(stat->mzxid));
  rb_hash_aset(hash, rb_str_new2("ctime"), UINT2NUM(stat->ctime));
  rb_hash_aset(hash, rb_str_new2("mtime"), UINT2NUM(stat->mtime));
  rb_hash_aset(hash, rb_str_new2("version"), INT2NUM(stat->version));
  rb_hash_aset(hash, rb_str_new2("cversion"), INT2NUM(stat->cversion));
  rb_hash_aset(hash, rb_str_new2("aversion"), INT2NUM(stat->aversion));
  rb_hash_aset(hash, rb_str_new2("ephemeralOwner"), UINT2NUM(stat->ephemeralOwner));
  rb_hash_aset(hash, rb_str_new2("dataLength"), INT2NUM(stat->dataLength));
  rb_hash_aset(hash, rb_str_new2("numChildren"), INT2NUM(stat->numChildren));
  rb_hash_aset(hash, rb_str_new2("pzxid"), UINT2NUM(stat->pzxid));
  return hash;
}

static VALUE method_get_acl(VALUE self, VALUE path) {
  FETCH_DATA_PTR(self, zk);
  Check_Type(path, T_STRING);

  //  ZOOAPI int zoo_get_acl(zhandle_t *zh, const char *path, struct ACL_vector *acl,
  //                     struct Stat *stat);
  struct ACL_vector acl;
  struct Stat stat;
  check_errors(zoo_get_acl(zk->zh, RSTRING(path)->ptr, &acl, &stat));

  VALUE result = rb_ary_new();
  rb_ary_push(result, acl_vector_to_ruby(&acl));
  rb_ary_push(result, stat_to_ruby(&stat));
  return result;
}

static VALUE method_is_unrecoverable(VALUE self) {
  FETCH_DATA_PTR(self, zk);
  if(is_unrecoverable(zk->zh) == ZINVALIDSTATE)
    return Qtrue;

  return Qfalse;
}

static VALUE method_recv_timeout(VALUE self) {
  FETCH_DATA_PTR(self, zk);
  return INT2NUM(zoo_recv_timeout(zk->zh));
}

#warning [emaland] make this a class method or global
static VALUE method_set_debug_level(VALUE self, VALUE level) {
  FETCH_DATA_PTR(self, zk);
  Check_Type(level, T_FIXNUM);
  zoo_set_debug_level(FIX2INT(level));
  return Qnil;
}

#warning [emaland] make this a class method or global
static VALUE method_zerror(VALUE self, VALUE errc) {
  return rb_str_new2(zerror(FIX2INT(errc)));
}

static VALUE method_state(VALUE self) {
  FETCH_DATA_PTR(self, zk);
  return INT2NUM(zoo_state(zk->zh));
}

#warning [emaland] make this a class method or global
static VALUE method_set_log_stream(VALUE self, VALUE stream) {
  // convert stream to FILE*
  FILE *fp_stream = (FILE*)stream;
  zoo_set_log_stream(fp_stream);
  return Qnil;
}

static VALUE method_set_watcher(VALUE self, VALUE new_watcher) {
  FETCH_DATA_PTR(self, zk);
#warning [emaland] needs to be tested/implemented
  return Qnil;
  //  watcher_fn old_watcher = zoo_set_watcher(zk->zh, new_watcher);
  //  return old_watcher;
}

void Init_zookeeper_c() {
  ZooKeeper = rb_define_class("CZooKeeper", rb_cObject);

#define DEFINE_METHOD(method, args) { \
    rb_define_method(ZooKeeper, #method, method_ ## method, args); }

  DEFINE_METHOD(initialize, 1);
  DEFINE_METHOD(get_children, 1);
  DEFINE_METHOD(exists, 2);
  DEFINE_METHOD(create, 3);
  DEFINE_METHOD(delete, 2);
  DEFINE_METHOD(get, 1);
  DEFINE_METHOD(set, -1);

  /* TODO */
  DEFINE_METHOD(add_auth, 3);
  DEFINE_METHOD(set_acl, -1);
  DEFINE_METHOD(async, 1);
  DEFINE_METHOD(client_id, 0);
  DEFINE_METHOD(close, 0);
  DEFINE_METHOD(deterministic_conn_order, 1);
  DEFINE_METHOD(get_acl, 2);
  DEFINE_METHOD(is_unrecoverable, 0);
  DEFINE_METHOD(recv_timeout, 1);
  DEFINE_METHOD(set2, -1);
  DEFINE_METHOD(set_debug_level, 1);
  DEFINE_METHOD(set_log_stream, 1);
  DEFINE_METHOD(set_watcher, 2);
  DEFINE_METHOD(state, 0);
  DEFINE_METHOD(zerror, 1);

  eNoNode = rb_define_class_under(ZooKeeper, "NoNodeError", rb_eRuntimeError);
  eBadVersion = rb_define_class_under(ZooKeeper, "BadVersionError", rb_eRuntimeError);

#define EXPORT_CONST(x) { rb_define_const(ZooKeeper, #x, INT2FIX(x)); }

  /* create flags */
  EXPORT_CONST(ZOO_EPHEMERAL);
  EXPORT_CONST(ZOO_SEQUENCE);

  /* 
     session state
  */
  EXPORT_CONST(ZOO_EXPIRED_SESSION_STATE);
  EXPORT_CONST(ZOO_AUTH_FAILED_STATE);
  EXPORT_CONST(ZOO_CONNECTING_STATE);
  EXPORT_CONST(ZOO_ASSOCIATING_STATE);
  EXPORT_CONST(ZOO_CONNECTED_STATE);

  /* notifications */
  EXPORT_CONST(ZOOKEEPER_WRITE);
  EXPORT_CONST(ZOOKEEPER_READ);

  /* errors */
  EXPORT_CONST(ZOK);
  EXPORT_CONST(ZSYSTEMERROR);
  EXPORT_CONST(ZRUNTIMEINCONSISTENCY);
  EXPORT_CONST(ZDATAINCONSISTENCY);
  EXPORT_CONST(ZCONNECTIONLOSS);
  EXPORT_CONST(ZMARSHALLINGERROR);
  EXPORT_CONST(ZUNIMPLEMENTED);
  EXPORT_CONST(ZOPERATIONTIMEOUT);
  EXPORT_CONST(ZBADARGUMENTS);
  EXPORT_CONST(ZINVALIDSTATE);

  /** API errors. */
  EXPORT_CONST(ZAPIERROR);
  EXPORT_CONST(ZNONODE);
  EXPORT_CONST(ZNOAUTH);
  EXPORT_CONST(ZBADVERSION);
  EXPORT_CONST(ZNOCHILDRENFOREPHEMERALS);
  EXPORT_CONST(ZNODEEXISTS);
  EXPORT_CONST(ZNOTEMPTY);
  EXPORT_CONST(ZSESSIONEXPIRED);
  EXPORT_CONST(ZINVALIDCALLBACK);
  EXPORT_CONST(ZINVALIDACL);
  EXPORT_CONST(ZAUTHFAILED);
  EXPORT_CONST(ZCLOSING);
  EXPORT_CONST(ZNOTHING);
  EXPORT_CONST(ZSESSIONMOVED);

  /* debug levels */
  EXPORT_CONST(ZOO_LOG_LEVEL_ERROR);
  EXPORT_CONST(ZOO_LOG_LEVEL_WARN);
  EXPORT_CONST(ZOO_LOG_LEVEL_INFO);
  EXPORT_CONST(ZOO_LOG_LEVEL_DEBUG);

  /* ACL constants */
  EXPORT_CONST(ZOO_PERM_READ);
  EXPORT_CONST(ZOO_PERM_WRITE);
  EXPORT_CONST(ZOO_PERM_CREATE);
  EXPORT_CONST(ZOO_PERM_DELETE);
  EXPORT_CONST(ZOO_PERM_ADMIN);
  EXPORT_CONST(ZOO_PERM_ALL);

  /* Watch types */
  EXPORT_CONST(ZOO_CREATED_EVENT);
  EXPORT_CONST(ZOO_DELETED_EVENT);
  EXPORT_CONST(ZOO_CHANGED_EVENT);
  EXPORT_CONST(ZOO_CHILD_EVENT);
  EXPORT_CONST(ZOO_SESSION_EVENT);
  EXPORT_CONST(ZOO_NOTWATCHING_EVENT);
  
}
