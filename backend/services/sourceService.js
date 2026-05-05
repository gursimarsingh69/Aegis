const supabase = require('../config/supabase');

/**
 * Source Service — manages trusted platform sources.
 */
const sourceService = {
  /**
   * Register a new trusted source.
   * @param {{ platform_name: string, url: string }} data
   * @returns {Promise<object>}
   */
  async create({ platform_name, url }) {
    const { data, error } = await supabase
      .from('sources')
      .insert({ platform_name, url })
      .select()
      .single();

    if (error) throw error;
    return data;
  },

  /**
   * Fetch all trusted sources.
   * @returns {Promise<object[]>}
   */
  async getAll() {
    const { data, error } = await supabase
      .from('sources')
      .select('*')
      .order('created_at', { ascending: false });

    if (error) throw error;
    return data;
  },

  /**
   * Check if a given URL belongs to a trusted source.
   * Extracts the hostname from the URL and checks against stored source URLs.
   * @param {string} urlToCheck
   * @returns {Promise<boolean>}
   */
  async isTrusted(urlToCheck) {
    try {
      const hostname = new URL(urlToCheck).hostname;

      const { data, error } = await supabase
        .from('sources')
        .select('url');

      if (error) throw error;
      if (!data || data.length === 0) return false;

      // Check if any trusted source URL's hostname matches
      return data.some((source) => {
        try {
          const sourceHostname = new URL(source.url).hostname;
          return hostname === sourceHostname || hostname.endsWith(`.${sourceHostname}`);
        } catch {
          return false;
        }
      });
    } catch {
      return false;
    }
  },
};

module.exports = sourceService;
