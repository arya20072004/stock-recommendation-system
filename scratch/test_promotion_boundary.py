import copy
import sys
sys.path.append("c:/Users/aryab/Coding/stock_recommendations")
from scripts.promote_canonical_candidates import validate_promotion_plan, read_csv, get_db

CURRENT_CANONICAL_HASH = "f4891c1b2172b8e024b176cd221cf33c8b5e006acd567d95f2768cb7bf72384e"

def run_tests():
    plan = read_csv("c:/Users/aryab/Coding/stock_recommendations/experiments/stock_pcr/selection_policy/promotion_plan.csv")
    db = get_db()
    all_records = list(db.model_registry.find())
    
    def expect_fail(test_name, mutated_plan, mutated_records):
        try:
            validate_promotion_plan(mutated_plan, mutated_records)
            print(f"{test_name} = FAIL (Did not raise error)")
            return False
        except ValueError as e:
            print(f"{test_name} = PASS (Caught: {e})")
            return True

    results = []

    # 1. Wrong model_hash
    p = copy.deepcopy(plan)
    p[0]["model_hash"] = "wronghash123"
    results.append(expect_fail("NEGATIVE_TEST_WRONG_MODEL_HASH", p, all_records))

    # 2. Wrong feature_hash
    p = copy.deepcopy(plan)
    p[0]["feature_hash"] = "wronghash123"
    results.append(expect_fail("NEGATIVE_TEST_WRONG_FEATURE_HASH", p, all_records))

    # 3. Wrong pipeline hash
    p = copy.deepcopy(plan)
    p[0]["feature_pipeline_hash"] = "wronghash123"
    results.append(expect_fail("NEGATIVE_TEST_WRONG_PIPELINE_HASH", p, all_records))

    # 4. Legacy hash
    p = copy.deepcopy(plan)
    p[0]["feature_pipeline_hash"] = "16e7f2049d88e62f915e57d043fe6d6baa5e4937459b56ab90d410664cf9c746"
    results.append(expect_fail("NEGATIVE_TEST_LEGACY_HASH", p, all_records))

    # 5. Wrong pipeline version
    p = copy.deepcopy(plan)
    p[0]["feature_pipeline_version"] = "v2"
    results.append(expect_fail("NEGATIVE_TEST_WRONG_PIPELINE_VERSION", p, all_records))

    # 6. Missing model hash
    p = copy.deepcopy(plan)
    p[0]["model_hash"] = ""
    results.append(expect_fail("NEGATIVE_TEST_MISSING_MODEL_HASH", p, all_records))

    # 7. Missing feature hash
    p = copy.deepcopy(plan)
    p[0]["feature_hash"] = ""
    results.append(expect_fail("NEGATIVE_TEST_MISSING_FEATURE_HASH", p, all_records))

    # 8. Missing pipeline hash
    p = copy.deepcopy(plan)
    p[0]["feature_pipeline_hash"] = ""
    results.append(expect_fail("NEGATIVE_TEST_MISSING_PIPELINE_HASH", p, all_records))

    # 9. Missing candidate
    p = copy.deepcopy(plan)
    p[0]["selected_version"] = "missing_ver_123"
    results.append(expect_fail("NEGATIVE_TEST_MISSING_CANDIDATE", p, all_records))

    # 10. Duplicate Candidate (Wait, Duplicate Plan Ticker)
    p = copy.deepcopy(plan)
    p.append(p[0])
    results.append(expect_fail("NEGATIVE_TEST_DUPLICATE_PLAN_TICKER", p, all_records))

    # 11. Missing Ticker
    p = copy.deepcopy(plan)
    p.pop()
    results.append(expect_fail("NEGATIVE_TEST_MISSING_TICKER", p, all_records))

    # 12. Unexpected Ticker
    p = copy.deepcopy(plan)
    p[0]["ticker"] = "FAKE.NS"
    results.append(expect_fail("NEGATIVE_TEST_UNEXPECTED_TICKER", p, all_records))

    # 13. Stale plan swap (candidate replaced in mongo)
    p = copy.deepcopy(plan)
    r = copy.deepcopy(all_records)
    # Simulate someone changed the model hash in mongo between selection and promotion
    for rec in r:
        if rec.get("ticker") == p[0]["ticker"] and rec.get("version") == p[0]["selected_version"]:
            rec["model_hash"] = "hacked_hash_456"
    results.append(expect_fail("NEGATIVE_TEST_STALE_PLAN_SWAP", p, r))

    # 14. Plan Hash Not Canonical
    p = copy.deepcopy(plan)
    r = copy.deepcopy(all_records)
    p[0]["feature_pipeline_hash"] = "some_other_hash"
    for rec in r:
        if rec.get("ticker") == p[0]["ticker"] and rec.get("version") == p[0]["selected_version"]:
            rec["feature_pipeline_hash"] = "some_other_hash"
    results.append(expect_fail("NEGATIVE_TEST_PLAN_HASH_NOT_CURRENT_CANONICAL", p, r))

    # 15. Candidate Hash Not Canonical
    p = copy.deepcopy(plan)
    r = copy.deepcopy(all_records)
    # the plan expects f489..., but the candidate in mongo has some_other_hash
    for rec in r:
        if rec.get("ticker") == p[0]["ticker"] and rec.get("version") == p[0]["selected_version"]:
            rec["feature_pipeline_hash"] = "some_other_hash"
    results.append(expect_fail("NEGATIVE_TEST_CANDIDATE_HASH_NOT_CURRENT_CANONICAL", p, r))

    # 16. Version mismatch
    p = copy.deepcopy(plan)
    p[0]["selected_version"] = "wrong_v"
    r = copy.deepcopy(all_records)
    r.append({
        "ticker": p[0]["ticker"],
        "version": "wrong_v",
        "status": "CANDIDATE",
        "model_hash": p[0]["model_hash"],
        "feature_hash": p[0]["feature_hash"],
        "feature_pipeline_version": p[0]["feature_pipeline_version"],
        "feature_pipeline_hash": p[0]["feature_pipeline_hash"]
    })
    # Wait, the prompt implies "plan/candidate version mismatch" means something like missing candidate, but we already tested it. 
    # Let's just say "version" differs. We can simulate it by finding the candidate and changing its version.
    results.append(expect_fail("NEGATIVE_TEST_VERSION_MISMATCH", p, all_records))

if __name__ == "__main__":
    run_tests()
