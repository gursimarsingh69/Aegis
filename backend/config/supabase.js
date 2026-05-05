const { createClient } = require('@supabase/supabase-js');
const { supabaseUrl, supabaseKey } = require('./env');

// Single shared Supabase client instance
const supabase = createClient(supabaseUrl, supabaseKey);

module.exports = supabase;
