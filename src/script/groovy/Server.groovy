import javax.ws.rs.*
import javax.ws.rs.core.*
import com.sun.jersey.api.core.*
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory
import org.glassfish.grizzly.http.server.HttpServer

@GrabConfig(systemClassLoader = true)
@GrabResolver(name = 'gretty', root = 'http://groovypp.artifactoryonline.com/groovypp/libs-releases-local')
@Grapes([
        @Grab('com.sun.jersey:jersey-server:1.12'),
        @Grab('com.sun.jersey:jersey-core:1.12'),
        @Grab(group='com.sun.jersey', module='jersey-grizzly2', version='1.12'),
        @Grab(group='javax.ws.rs', module='jsr311-api', version='1.1.1')])

@Path("/{code}")
class Main {

    @GET @Produces("text/plain")
    public Response getUserByCode(@PathParam('code') String code) {
        return Response.ok().entity("Hello $code world".toString()).build();
    }

    public static startServer() {
        ResourceConfig resources = new ClassNamesResourceConfig(Main)
        def uri = UriBuilder.fromUri("http://localhost/").port(6789).build();
        HttpServer httpServer = GrizzlyServerFactory.createHttpServer(uri, resources);
        println("Jersey app started with WADL available at ${uri}")
        System.in.read();
        httpServer.stop();
    }
}

Main.startServer()
