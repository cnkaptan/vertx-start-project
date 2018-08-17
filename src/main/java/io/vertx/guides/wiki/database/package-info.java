// There is one last step required before the proxy code generation works: the service package needs
// to have a package-info.java annotated to define a Vert.x module:

@ModuleGen(groupPackage = "io.vertx.guides.wiki.database", name = "wiki-database")
package io.vertx.guides.wiki.database;

import io.vertx.codegen.annotations.ModuleGen;