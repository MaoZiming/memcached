#pragma once

#include <cmath>
#include <random>
#include <vector>

class FastZipf
{
public:
    FastZipf(double alpha, int n) : alpha(alpha), n(n)
    {
        // Precompute the normalization constant
        zeta_n = 0.0;
        for (int i = 1; i <= n; i++)
        {
            zeta_n += 1.0 / std::pow(i, alpha);
        }
    }

    int zipf()
    {
        double sum_prob = 0.0;
        double rand_val = uniform_dist(generator) * zeta_n;

        for (int i = 1; i <= n; i++)
        {
            sum_prob += 1.0 / std::pow(i, alpha);
            if (sum_prob >= rand_val)
            {
                return i;
            }
        }

        // In case of numerical precision issues, return n
        return n;
    }

    std::vector<int> generate_zipf(int num_operations)
    {
        std::vector<int> results;
        results.reserve(num_operations);
        for (int i = 0; i < num_operations; i++)
        {
            results.push_back(zipf());
        }
        return results;
    }

private:
    double alpha;
    int n;
    double zeta_n;
    std::default_random_engine generator;
    std::uniform_real_distribution<double> uniform_dist{0.0, 1.0};
};