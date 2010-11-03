/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.jpa;

import javax.persistence.EntityManager;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ParamChecker;

/**
 * Persist the CoordinatorJob bean.
 */
public class CoordJobInsertCommand implements JPACommand<String> {

    private CoordinatorJobBean coordJob = null;

    public CoordJobInsertCommand(CoordinatorJobBean coordJob) {
        ParamChecker.notNull(coordJob, "coordJob");
        this.coordJob = coordJob;
    }

    @Override
    public String getName() {
    	return "CoordJobInsertCommand";
    }

    @Override
    public String execute(EntityManager em) throws CommandException {
        em.persist(coordJob);
        return null;
    }
}