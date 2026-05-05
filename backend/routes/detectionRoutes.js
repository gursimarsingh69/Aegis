const express = require('express');
const router = express.Router();
const detectionController = require('../controllers/detectionController');

router.get('/',    detectionController.getAll);
router.get('/:id', detectionController.getById);
router.post('/',   detectionController.create);

module.exports = router;
