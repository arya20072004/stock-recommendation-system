import unittest
from src.data.news_collector import clean_html_description, is_description_meaningful

class TestNormalizer(unittest.TestCase):
    def test_plain_text(self):
        desc = "This is a plain text description."
        self.assertEqual(clean_html_description(desc), desc)
        self.assertTrue(is_description_meaningful(clean_html_description(desc), "Title", "Source"))
        
    def test_html_anchor(self):
        desc = '<a href="https://example.com">Example Link</a>'
        self.assertEqual(clean_html_description(desc), "Example Link")
        self.assertFalse(is_description_meaningful(clean_html_description(desc), "Title", "Source")) # Too short
        
    def test_nested_html(self):
        desc = '<div><p>This is <b>nested</b> HTML.</p></div>'
        clean = clean_html_description(desc)
        self.assertEqual(clean, "This is nested HTML.")
        self.assertTrue(is_description_meaningful(clean, "Title", "Source"))
        
    def test_html_entities(self):
        desc = "AT&amp;T &quot;Earnings&quot;"
        self.assertEqual(clean_html_description(desc), "AT&T \"Earnings\"")
        
    def test_raw_url_only(self):
        desc = "https://example.com/article/123"
        self.assertEqual(clean_html_description(desc), desc)
        self.assertFalse(is_description_meaningful(clean_html_description(desc), "Title", "Source"))
        
    def test_google_rss_redirect_url(self):
        desc = "https://news.google.com/rss/articles/CBMi..."
        self.assertEqual(clean_html_description(desc), desc)
        self.assertFalse(is_description_meaningful(clean_html_description(desc), "Title", "Source"))
        
    def test_empty_string(self):
        desc = ""
        self.assertIsNone(clean_html_description(desc))
        self.assertFalse(is_description_meaningful(clean_html_description(desc), "Title", "Source"))
        
    def test_none(self):
        self.assertIsNone(clean_html_description(None))
        self.assertFalse(is_description_meaningful(clean_html_description(None), "Title", "Source"))
        
    def test_title_duplicated(self):
        desc = "Reliance Industries Q3 Results"
        title = "Reliance Industries Q3 Results"
        self.assertEqual(clean_html_description(desc), desc)
        self.assertFalse(is_description_meaningful(clean_html_description(desc), title, "Source"))
        
    def test_legitimate_punctuation(self):
        desc = "Reliance Industries Q3 Results: Net profit rises 15%, beats estimates."
        clean = clean_html_description(desc)
        self.assertEqual(clean, desc)
        self.assertTrue(is_description_meaningful(clean, "Different Title", "Source"))
        
    def test_multiple_whitespace(self):
        desc = "This   has \n\n multiple \t spaces."
        self.assertEqual(clean_html_description(desc), "This has multiple spaces.")
        
    def test_unicode(self):
        desc = "₹100 Crore profit announced by Company™"
        self.assertEqual(clean_html_description(desc), desc)
        self.assertTrue(is_description_meaningful(clean_html_description(desc), "Title", "Source"))
        
if __name__ == '__main__':
    unittest.main()
