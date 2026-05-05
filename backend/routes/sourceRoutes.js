const express = require('express');
const router = express.Router();
const sourceController = require('../controllers/sourceController');

router.get('/',  sourceController.getAll);
router.post('/', sourceController.create);

module.exports = router;
