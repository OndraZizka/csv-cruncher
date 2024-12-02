
Releasing
=========

**Edit 2024-11:** 

1) Sonatype has changed the way to release. Will eventually need some changes in the process.

2) The current pom does not attach the basic .jar, so it is not signed by PHP.  
   A workaround is to sign it manually, upload all files to staging, and release that way.
   ```shell
   gpg --sign '--local-user' '73BA361CA92D829800730DE207838E30786B5257' '--armor' '--detach-sign' '--output' '/home/o/uw/csv-cruncher/target/csv-cruncher-2.7.1.jar.asc'  '/home/o/uw/csv-cruncher/target/csv-cruncher-2.7.1.jar'
   ```

*********

How to release
--------------

1) Need the PGP key, see `gpg -K`, `gpg --export-secret-keys` and `gpg --import`.
       
       gpg: key 07838E30786B5257: public key "Ondrej Zizka (Jar signing for Maven Central) <zizka@seznam.cz>" imported

2) On MacOS, to prevent `gpg: signing failed: Inappropriate ioctl for device `, run:

   ```bash
       export GPG_TTY=$(tty)
   ```
   
3) To do the actual tag:

   ```bash
       mvn release:prepare -Prelease  ## -Possrh removed, see pom.xml
       mvn release:perform -Prelease  ## -Possrh removed, see pom.xml
   ```

    This should include invocation of the `deploy` plugin.  
    If not, or if that fails, it's possible to stage the artifacts manually.   
    
    Switch to the release tag with the non-SNAPSHOT version to publish, and run:

   ```bash
       mvn clean deploy -Possrh -Prelease   # p..##
   ```

4) Go to [Sonatype OSSRH](https://oss.sonatype.org/#stagingRepositories) and release the staged snapshot:
   * Close the freshly created staging repo.
   * Wait a minute.
   * If it passed validation, promote the staging repo.

5) Go to GitHub and [create a new release](https://github.com/OndraZizka/csv-cruncher/releases/new):
   * Name is like `<version> <main new feature>`
   * Description describes what's new, and contains the usage example
   * Attach the files `csv-cruncher-<version>-dist.jar` and `...-fatjar.jar` 
