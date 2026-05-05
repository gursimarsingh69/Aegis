const express = require('express');
const router = express.Router();

const assetRoutes     = require('./assetRoutes');
const detectionRoutes = require('./detectionRoutes');
const alertRoutes     = require('./alertRoutes');
const sourceRoutes    = require('./sourceRoutes');

router.use('/assets',     assetRoutes);
router.use('/detections', detectionRoutes);
router.use('/alerts',     alertRoutes);
router.use('/sources',    sourceRoutes);

module.exports = router;
