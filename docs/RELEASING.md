
Releasing
=========

1) Need the PGP key, see `gpg -K`, `gpg --export-secret-keys` and `gpg --import-secret-keys`
       
       gpg: key 07838E30786B5257: public key "Ondrej Zizka (Jar signing for Maven Central) <zizka@seznam.cz>" imported

2) On MacOS, to prevent `gpg: signing failed: Inappropriate ioctl for device `, run:

       $ export GPG_TTY=$(tty)

3) To do the actual tag:

       $ mvn release:prepare -Possrh -Prelease
       $ mvn release:perform -Possrh -Prelease

    This should include invocation of the `deploy` plugin.  
    If not, it's possible to do it manually (with `autoReleaseAfterClose` = true and a non-SNAPSHOT version to publish):
    
       $ mvn clean deploy -Possrh # p..##

4) Go to [Sonatype OSSRH](https://oss.sonatype.org/#stagingRepositories) and release the staged snapshot:
   * Close the freshly created staging repo
   * Wait several minutes
   * If it passed validation, promote the staging repo.

5) Go to GitHub and [create a new release](https://github.com/OndraZizka/csv-cruncher/releases/new):
   * Name is like `<version> <main new feature>`
   * Description describes what's new, and contains the usage example
   * Attach the files `csv-cruncher-<version>-dist.jar` and `...-single.jar` 
