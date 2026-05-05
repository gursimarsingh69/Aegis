const express = require('express');
const router = express.Router();
const alertController = require('../controllers/alertController');

router.get('/',    alertController.getAll);
router.get('/:id', alertController.getById);
router.post('/',   alertController.create);

module.exports = router;
