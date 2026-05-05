/**
 * Global error handler middleware.
 * Catches all errors forwarded via next(err) and returns a consistent JSON response.
 */
function errorHandler(err, req, res, _next) {
  // Log the full error in development
  if (process.env.NODE_ENV !== 'production') {
    console.error('🔥 Error:', err);
  }

  // Supabase errors have a `code` and `message` property
  if (err.code) {
    const statusMap = {
      '23503': 400, // foreign key violation
      '23505': 409, // unique violation
      '23514': 400, // check constraint violation
      'PGRST116': 404, // row not found (PostgREST)
    };

    const status = statusMap[err.code] || 500;

    return res.status(status).json({
      success: false,
      error: err.message || 'Database error',
      code: err.code,
    });
  }

  // Generic errors
  const status = err.statusCode || err.status || 500;
  return res.status(status).json({
    success: false,
    error: err.message || 'Internal Server Error',
  });
}

module.exports = errorHandler;
