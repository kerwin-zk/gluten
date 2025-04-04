/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <optional>
#include <Parser/RelParsers/RelParser.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{
class FilterRelParser : public RelParser
{
public:
    explicit FilterRelParser(ParserContextPtr parser_context_);
    ~FilterRelParser() override = default;

    std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel & rel) override { return &rel.filter().input(); }

    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;

private:
    // Poco::Logger * logger = &Poco::Logger::get("ProjectRelParser");
};
}
