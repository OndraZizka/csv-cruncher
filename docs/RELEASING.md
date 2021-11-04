
Releasing
=========

1) Need the PGP key, see `gpg -K`, `gpg --export-secret-keys` and `gpg --import-secret-keys`
       
       gpg: key 07838E30786B5257: public key "Ondrej Zizka (Jar signing for Maven Central) <zizka@seznam.cz>" imported

2) On MacOS, to prevent `gpg: signing failed: Inappropriate ioctl for device `, run:

       $ export GPG_TTY=$(tty)

3) To do the actual tag:

       $ mvn release:prepare -Possrh
       $ mvn release:perform -Possrh

    This should include invocation of the `deploy` plugin.  
    If not, it's possible to do it manually (with `autoReleaseAfterClose` = true and a non-SNAPSHOT version to publish):
    
       $ mvn clean deploy -Possrh # p..##

4) Go to [Sonatype OSSRH](https://oss.sonatype.org/) and release the staged snapshot.
