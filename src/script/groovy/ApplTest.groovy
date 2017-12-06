import groovy.transform.Field
import static Lala.*

@Field
@CommandLineArgument(shortKey = "f")
String fooArgument = ""

@Field
@CommandLineArgument(shortKey = "a", description = "super integer")
Integer anotherArgument = 12

@Field
@CommandLineArgument(shortKey = "t", description = "enable this")
Boolean doThis = false

OverrideFieldsByCommandLineArguments.forScript(this)
println("fooArgument: $fooArgument")

println(HA)

class Lala {
    final static String HA = "ha! ha!"
}