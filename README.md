# Antidote file system

A [FUSE][fuse-wiki] file system backed by [Antidote][antidote].   

In the [_fuse-hl](_fuse-hl/) folder is an early implementation using 
the [FUSE synchronous high-level API][fuse-hl].  
Instead, this implementation uses the [FUSE asynchronous low-level API][fuse-ll]
by means of its node.js bindings.  
More details on the design are available in the `doc` folder.


## Getting started

Requirements: [node.js 8][nodejs], [npm][npm], [Antidote][antidote-setup], 
[Fuse 2.9][fuse].  
To compile it: `npm install`.  
To run it: `node src/antidote-fs.js -m <mount_dir> -a <antidote_address:port>`.  


## Credits

[RainbowFS][rainbowfs] research project.

 [antidote]: http://syncfree.github.io/antidote/
 [fuse-wiki]: https://en.wikipedia.org/wiki/Filesystem_in_Userspace
 [rainbowfs]: http://rainbowfs.lip6.fr/
 [nodejs]: https://nodejs.org/
 [npm]: https://www.npmjs.com/
 [antidote-setup]: http://syncfree.github.io/antidote/setup.html
 [fuse]: https://github.com/libfuse/libfuse
 [fuse-hl]: http://libfuse.github.io/doxygen/structfuse__operations.html
 [fuse-ll]: http://libfuse.github.io/doxygen/structfuse__lowlevel__ops.html
