const fs = require("fs")
const stream = require('node:stream')

async function uploadChunk(
  openStream, //: () => NodeJS.ReadableStream,
  start,
  end,
) { //: Promise<void> {
  const data = openStream()
  const ws = new stream.PassThrough()
  const p = await data.pipe(ws)
  console.log(`pipe ${start} - ${end}`)
  return p
}

let done = false

const maxChunkSize = 256

const uploadFile = async (archivePath) => {
  const fd = fs.openSync(archivePath, "r");
  const fileSize = fs.statSync(archivePath).size;

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
          )
        }
      })
    );
  } finally {
    console.log('close')
    fs.closeSync(fd);
  }
  done = true;
};

const wait = (next) => setTimeout(() => {
  if (!done) {
    console.log("wait...")
    next()
  }
}, 1000)

uploadFile(process.argv[1])

wait(wait)


