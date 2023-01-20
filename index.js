const fs = require("fs");
const stream = require("node:stream");

const uploadWithCallback = (
  openStream, //: () => NodeJS.ReadableStream,
  start,
  end,
  cb
) => {
  const data = openStream()
  const ws = new stream.PassThrough()
  data.on('end', function() {
    console.log(`pipe end ${start}-${end}`)
    cb(null)
  })
 // console.log(`pipe start ${start}-${end}`)
  data.pipe(ws)
}

let count = 0
const uploadChunk = (
  openStream, //: () => NodeJS.ReadableStream,
  start,
  end
) => new Promise((resolve, reject) =>{
  if (count++ < 0) {
    reject(Error("AAA"))
  } else {
    uploadWithCallback(openStream, start, end, resolve);
  }
})

let done = false

const maxChunkSize = 128

const uploadFile = async (archivePath) => {
  const fd = fs.openSync(archivePath, "r")
  const fileSize = fs.statSync(archivePath).size
  console.log(archivePath + ' fileSize '+fileSize)

  const parallelUploads = [...new Array(4).keys()];

  let offset = 0;

  try {
    await Promise.all(
      parallelUploads.map(async () => {
        while (offset < fileSize) {
          const chunkSize = Math.min(fileSize - offset, maxChunkSize);
          const start = offset;
          const end = offset + chunkSize - 1;
          offset += maxChunkSize;

          await uploadChunk(
            () =>
              fs
                .createReadStream(archivePath, {
                  fd,
                  start,
                  end,
                  autoClose: false
                })
                .on("error", error => {
                  throw new Error(
                    `Cache upload failed because file read failed with ${error.message}`
                  );
                }),
            start,
            end
          );
        }
      })
    );
  } finally {
    console.log("close");
    fs.closeSync(fd);
  }
  done = true;
};

const wait = () => setTimeout(() => {
  if (!done) {
    console.log("wait...");
    wait();
  }
}, 10);

console.log('start upload '+process.argv)
uploadFile(process.argv[2]);
console.log('start wait')

//wait();


