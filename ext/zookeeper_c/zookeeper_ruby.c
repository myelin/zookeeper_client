/* Ruby wrapper for the ZooKeeper C API
 * Phillip Pearson <pp@myelin.co.nz>
 */

#define THREADED

#include "ruby.h"

#include "zookeeper.h"
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

static void check_errors(int rc) {
  switch (rc) {
  case ZOK: /* all good! */ break;
  case ZBADARGUMENTS: rb_raise(rb_eRuntimeError, "invalid input parameters");
  case ZMARSHALLINGERROR: rb_raise(rb_eRuntimeError, "failed to marshall a request; possibly out of memory");
  case ZOPERATIONTIMEOUT: rb_raise(rb_eRuntimeError, "failed to flush the buffers within the specified timeout");
  case ZCONNECTIONLOSS: rb_raise(rb_eRuntimeError, "a network error occured while attempting to send request to server");
  case ZSYSTEMERROR: rb_raise(rb_eRuntimeError, "a system (OS) error occured; it's worth checking errno to get details");
  case ZNONODE: rb_raise(eNoNode, "the node does not exist");
  case ZNOAUTH: rb_raise(rb_eRuntimeError, "the client does not have permission");
  case ZBADVERSION: rb_raise(eBadVersion, "expected version does not match actual version");
  case ZINVALIDSTATE: rb_raise(rb_eRuntimeError, "zhandle state is either SESSION_EXPIRED_STATE or AUTH_FAILED_STATE");
  case ZNODEEXISTS: rb_raise(rb_eRuntimeError, "the node already exists");
  case ZNOCHILDRENFOREPHEMERALS: rb_raise(rb_eRuntimeError, "cannot create children of ephemeral nodes");
  case ZINVALIDACL: rb_raise(rb_eRuntimeError, "invalid ACL specified");
  default: rb_raise(rb_eRuntimeError, "unknown error returned from zookeeper: %d", rc);
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

static VALUE method_get_children(VALUE self, VALUE path) {
  struct zk_rb_data* zk;
  struct String_vector strings;
  int i;
  VALUE output;

  Check_Type(path, T_STRING);
  Data_Get_Struct(rb_iv_get(self, "@data"), struct zk_rb_data, zk);

  check_errors(zoo_get_children(zk->zh, RSTRING(path)->ptr, 0, &strings));

  output = rb_ary_new();
  for (i = 0; i < strings.count; ++i) {
    rb_ary_push(output, rb_str_new2(strings.data[i]));
  }
  return output;
}

static VALUE method_exists(VALUE self, VALUE path, VALUE watch) {
  struct zk_rb_data* zk;
  struct Stat stat;

  Check_Type(path, T_STRING);
  Data_Get_Struct(rb_iv_get(self, "@data"), struct zk_rb_data, zk);

  check_errors(zoo_exists(zk->zh, RSTRING(path)->ptr, (watch != Qfalse && watch != Qnil), &stat));

  return array_from_stat(&stat);
}

static VALUE method_create(VALUE self, VALUE path, VALUE value, VALUE flags) {
  struct zk_rb_data* zk;
  char realpath[10240];

  Check_Type(path, T_STRING);
  Check_Type(value, T_STRING);
  Check_Type(flags, T_FIXNUM);
  Data_Get_Struct(rb_iv_get(self, "@data"), struct zk_rb_data, zk);

  check_errors(zoo_create(zk->zh, RSTRING(path)->ptr, RSTRING(value)->ptr, RSTRING(value)->len,
			  &ZOO_OPEN_ACL_UNSAFE, FIX2INT(flags), realpath, 10240));

  return rb_str_new2(realpath);
}

static VALUE method_delete(VALUE self, VALUE path, VALUE version) {
  struct zk_rb_data* zk;

  Check_Type(path, T_STRING);
  Check_Type(version, T_FIXNUM);
  Data_Get_Struct(rb_iv_get(self, "@data"), struct zk_rb_data, zk);

  check_errors(zoo_delete(zk->zh, RSTRING(path)->ptr, FIX2INT(version)));

  return Qnil;
}

static VALUE method_get(VALUE self, VALUE path) {
  struct zk_rb_data* zk;
  char data[1024];
  int data_len = 1024;
  struct Stat stat;

  Check_Type(path, T_STRING);
  Data_Get_Struct(rb_iv_get(self, "@data"), struct zk_rb_data, zk);
  
  check_errors(zoo_get(zk->zh, RSTRING(path)->ptr, 0, data, &data_len, &stat));
  /*printf("got some data; version=%d\n", stat.version);*/

  return rb_ary_new3(2,
		     rb_str_new(data, data_len),
		     array_from_stat(&stat));
}

static VALUE method_set(int argc, VALUE* argv, VALUE self)
{
  VALUE v_path, v_data, v_version;
  struct zk_rb_data* zk;
  int real_version = -1;

  rb_scan_args(argc, argv, "21", &v_path, &v_data, &v_version);
  
  Check_Type(v_path, T_STRING);
  Check_Type(v_data, T_STRING);
  Check_Type(v_version, T_FIXNUM);

  if(!NIL_P(v_version))
    real_version = FIX2INT(v_version);

  Data_Get_Struct(rb_iv_get(self, "@data"), struct zk_rb_data, zk);
  
  check_errors(zoo_set(zk->zh, RSTRING(v_path)->ptr, 
                       RSTRING(v_data)->ptr, RSTRING(v_data)->len, 
                       FIX2INT(v_version)));

  return Qnil;
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
