from subprocess import call
import cherrypy

class Root(object):

    def _cp_dispatch(self, vpath):
        if len(vpath) == 1:
            cherrypy.request.params['cmd'] = vpath.pop()
            return self
        return vpath

    @cherrypy.expose
    def index(self,cmd):
        if cmd == "restart":
            call(["cp", "subs.save", "subs"])
            return "Service reactivated with success!"
        if cmd == "upgrade":
            call(["cp","subs.up","subs"])
            return "Service upgraded with success!"
        return "Hello World!"

if __name__ == '__main__':
   cherrypy.config.update(
    {'server.socket_host': '0.0.0.0'} )
   cherrypy.quickstart(Root(), '/')




