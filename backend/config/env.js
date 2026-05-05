const dotenv = require('dotenv');
const path = require('path');

// Load .env from project root
dotenv.config({ path: path.resolve(__dirname, '..', '.env') });

const requiredVars = ['SUPABASE_URL', 'SUPABASE_ANON_KEY'];

for (const varName of requiredVars) {
  if (!process.env[varName]) {
    console.error(`❌  Missing required env variable: ${varName}`);
    process.exit(1);
  }
}

module.exports = {
  supabaseUrl:         process.env.SUPABASE_URL,
  supabaseKey:         process.env.SUPABASE_ANON_KEY,
  port:                parseInt(process.env.PORT, 10) || 3000,
  nodeEnv:             process.env.NODE_ENV || 'development',
  similarityThreshold: parseFloat(process.env.SIMILARITY_THRESHOLD) || 80,
  aiEngineUrl:         process.env.AI_ENGINE_URL || 'http://localhost:8000',
};
