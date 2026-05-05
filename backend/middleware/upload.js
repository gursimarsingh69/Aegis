const multer = require('multer');

/**
 * Multer middleware — memory storage for file uploads.
 *
 * Files are kept in memory as Buffers (req.file.buffer).
 * This avoids disk writes on the backend; the file is forwarded
 * directly to the AI Engine via HTTP.
 */
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024, // 10 MB max
  },
  fileFilter: (_req, file, cb) => {
    // Accept images and videos only
    if (file.mimetype.startsWith('image/') || file.mimetype.startsWith('video/')) {
      cb(null, true);
    } else {
      cb(new Error('Only image and video files are accepted.'), false);
    }
  },
});

module.exports = upload;
